package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ivan-cunha/prism-format/internal/compression"
	"github.com/ivan-cunha/prism-format/internal/encoding"
	"github.com/ivan-cunha/prism-format/pkg/types"
)

type FileInfo struct {
	Version  uint16
	Created  int64
	Modified int64
	Schema   types.Schema
}

func (r *Reader) GetFileInfo() (FileInfo, error) {
	return FileInfo{
		Version:  r.header.Version,
		Created:  time.Now().Unix(),
		Modified: time.Now().Unix(),
		Schema:   r.schema,
	}, nil
}

var (
	ErrInvalidMagic   = errors.New("invalid magic number")
	ErrInvalidVersion = errors.New("unsupported version")
	ErrInvalidColumn  = errors.New("invalid column")
)

type Reader struct {
	r          io.ReadSeeker
	header     encoding.FileHeader
	schema     types.Schema
	columns    map[string]*ColumnBlock
	nullBitmap []byte
}

func NewReader(r io.ReadSeeker) (*Reader, error) {
	reader := &Reader{
		r:       r,
		columns: make(map[string]*ColumnBlock),
	}

	fmt.Println("Reading file header...")
	header, err := encoding.ReadHeader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}
	reader.header = header

	fmt.Printf("Reading schema (length=%d)...\n", reader.header.SchemaLen)
	schemaBytes := make([]byte, reader.header.SchemaLen)
	if _, err := io.ReadFull(r, schemaBytes); err != nil {
		return nil, fmt.Errorf("failed to read schema: %v", err)
	}
	if err := reader.schema.UnmarshalJSON(schemaBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %v", err)
	}

	// Read null bitmap
	fmt.Printf("Reading null bitmap (length=%d)...\n", reader.header.NullBitmapLen)
	reader.nullBitmap = make([]byte, reader.header.NullBitmapLen)
	if _, err := io.ReadFull(r, reader.nullBitmap); err != nil {
		return nil, fmt.Errorf("failed to read null bitmap: %v", err)
	}

	fmt.Printf("Reading column metadata for %d columns...\n", len(reader.schema.Columns))
	if err := reader.readColumnMetadata(); err != nil {
		return nil, fmt.Errorf("failed to read column metadata: %v", err)
	}

	return reader, nil
}

func getCurrentOffset(r io.ReadSeeker) int64 {
	offset, _ := r.Seek(0, io.SeekCurrent)
	return offset
}

func (r *Reader) readColumnMetadata() error {
	fmt.Printf("Starting to read metadata at offset %d\n", getCurrentOffset(r.r))
	for _, col := range r.schema.Columns {
		var metadata ColumnMetadata

		// Read type as uint8
		var typeVal uint8
		if err := binary.Read(r.r, binary.BigEndian, &typeVal); err != nil {
			return err
		}
		metadata.Type = types.DataType(typeVal)

		// Read name length and name
		var nameLen uint32
		if err := binary.Read(r.r, binary.BigEndian, &nameLen); err != nil {
			return err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := r.r.Read(nameBytes); err != nil {
			return err
		}
		metadata.Name = string(nameBytes)

		// Read remaining fixed-size fields
		if err := binary.Read(r.r, binary.BigEndian, &metadata.Offset); err != nil {
			return err
		}
		if err := binary.Read(r.r, binary.BigEndian, &metadata.Length); err != nil {
			return err
		}
		if err := binary.Read(r.r, binary.BigEndian, &metadata.CompressedSize); err != nil {
			return err
		}
		if err := binary.Read(r.r, binary.BigEndian, &metadata.NullCount); err != nil {
			return err
		}

		r.columns[col.Name] = &ColumnBlock{
			Metadata: metadata,
			Data:     nil,
		}
	}
	return nil
}

func (r *Reader) wrapWithNullable(values interface{}) (interface{}, error) {
	switch v := values.(type) {
	case []int32:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []int64:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []float32:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []float64:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []string:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []bool:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	case []time.Time:
		result := make([]types.NullableValue, len(v))
		for i, val := range v {
			result[i] = types.NullableValue{
				Value: val,
				Valid: !r.isNull(uint64(i)),
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported type for nullable wrapping: %T", values)
	}
}

func (r *Reader) ReadColumn(name string) (interface{}, error) {
	col, exists := r.columns[name]
	if !exists {
		return nil, ErrInvalidColumn
	}

	if err := r.loadColumnData(col); err != nil {
		return nil, err
	}

	// Get the column's schema to check if it's nullable
	var colSchema *types.Column
	for _, c := range r.schema.Columns {
		if c.Name == name {
			colSchema = &c
			break
		}
	}
	if colSchema == nil {
		return nil, fmt.Errorf("column %s not found in schema", name)
	}

	// If column is not nullable, just decode normally
	if !colSchema.Nullable {
		return r.decodeColumn(col)
	}

	// For nullable columns, wrap values in NullableValue structs
	rawValues, err := r.decodeColumn(col)
	if err != nil {
		return nil, err
	}

	return r.wrapWithNullable(rawValues)
}

func (r *Reader) loadColumnData(col *ColumnBlock) error {
	if col.Data != nil {
		return nil // Already loaded
	}

	if _, err := r.r.Seek(col.Metadata.Offset, io.SeekStart); err != nil {
		return err
	}

	compressed := make([]byte, col.Metadata.CompressedSize)
	if _, err := r.r.Read(compressed); err != nil {
		return err
	}

	var compressor compression.Compressor
	var err error

	switch col.Metadata.Type {
	case types.TimestampType, types.DateType:
		compressor, err = compression.GetCompressor("temporal")
		if err != nil {
			compressor, err = compression.GetCompressor("snappy")
		}
	case types.Int32Type, types.Int64Type:
		compressor, err = compression.GetCompressor("delta")
		if err != nil {
			compressor, err = compression.GetCompressor("snappy")
		}
	case types.BooleanType:
		compressor, err = compression.GetCompressor("boolean")
		if err != nil {
			compressor, err = compression.GetCompressor("snappy")
		}
	default:
		compressor, err = compression.GetCompressor("snappy")
	}

	if err != nil {
		return fmt.Errorf("failed to get decompressor: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		return fmt.Errorf("decompression failed: %v", err)
	}

	col.Data = decompressed
	return nil
}

func (r *Reader) decodeColumn(col *ColumnBlock) (interface{}, error) {
	switch col.Metadata.Type {
	case types.Int32Type:
		return r.decodeInt32(col.Data)
	case types.Int64Type:
		return r.decodeInt64(col.Data)
	case types.Float32Type:
		return r.decodeFloat32(col.Data)
	case types.Float64Type:
		return r.decodeFloat64(col.Data)
	case types.StringType:
		return r.decodeString(col.Data)
	case types.BooleanType:
		return r.decodeBoolean(col.Data)
	case types.DateType:
		return r.decodeDate(col.Data)
	default:
		return nil, errors.New("unsupported type")
	}
}

func (r *Reader) decodeInt32(data []byte) ([]int32, error) {
	result := make([]int32, len(data)/4)
	for i := range result {
		result[i] = int32(binary.BigEndian.Uint32(data[i*4:]))
	}
	return result, nil
}

func (r *Reader) decodeInt64(data []byte) ([]int64, error) {
	result := make([]int64, len(data)/8)
	for i := range result {
		result[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return result, nil
}

func (r *Reader) decodeFloat32(data []byte) ([]float32, error) {
	result := make([]float32, len(data)/4)
	for i := range result {
		bits := binary.BigEndian.Uint32(data[i*4:])
		result[i] = float32FromBits(bits)
	}
	return result, nil
}

func (r *Reader) decodeFloat64(data []byte) ([]float64, error) {
	result := make([]float64, len(data)/8)
	for i := range result {
		bits := binary.BigEndian.Uint64(data[i*8:])
		result[i] = float64FromBits(bits)
	}
	return result, nil
}

func (r *Reader) decodeString(data []byte) ([]string, error) {
	dictLen := binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	dict := make([]string, dictLen)
	offset := 0
	for i := uint32(0); i < dictLen; i++ {
		strLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		dict[i] = string(data[offset : offset+int(strLen)])
		offset += int(strLen)
	}

	indexes := make([]uint32, (len(data)-offset)/4)
	for i := range indexes {
		indexes[i] = binary.BigEndian.Uint32(data[offset+i*4:])
	}

	result := make([]string, len(indexes))
	for i, idx := range indexes {
		if idx >= dictLen {
			return nil, errors.New("invalid string dictionary index")
		}
		result[i] = dict[idx]
	}

	return result, nil
}

func (r *Reader) decodeBoolean(data []byte) ([]bool, error) {
	result := make([]bool, len(data))
	for i := range result {
		result[i] = data[i] != 0
	}
	return result[:r.header.RowCount], nil
}

func (r *Reader) decodeDate(data []byte) ([]time.Time, error) {
	// Dates are stored as Unix timestamps (int64)
	timestamps, err := r.decodeInt64(data)
	if err != nil {
		return nil, err
	}

	result := make([]time.Time, len(timestamps))
	for i, ts := range timestamps {
		result[i] = time.Unix(ts, 0)
	}
	return result, nil
}

func float32FromBits(bits uint32) float32 {
	return float32(bits)
}

func float64FromBits(bits uint64) float64 {
	return float64(bits)
}

func (r *Reader) isNull(rowIndex uint64) bool {
	byteIndex := rowIndex / 8
	bitIndex := rowIndex % 8
	return (r.nullBitmap[byteIndex] & (1 << bitIndex)) != 0
}
