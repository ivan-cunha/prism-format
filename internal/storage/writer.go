package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ivan-cunha/prism-format/internal/compression"
	"github.com/ivan-cunha/prism-format/internal/encoding"
	"github.com/ivan-cunha/prism-format/pkg/types"
)

type Writer struct {
	w          io.Writer
	schema     types.Schema
	columns    []*ColumnBlock
	rowCount   uint64
	nullBitmap []byte
	bitmapSize uint64
}

func NewWriter(w io.Writer, schema types.Schema) *Writer {
	columns := make([]*ColumnBlock, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = NewColumnBlock(col.Name, col.Type)
	}

	return &Writer{
		w:          w,
		schema:     schema,
		columns:    columns,
		rowCount:   0,
		nullBitmap: make([]byte, 1024), // Initial size
		bitmapSize: 0,
	}
}

func (w *Writer) ensureNullBitmapCapacity(rowIndex uint64) {
	// Calculate required size in bytes
	requiredSize := (rowIndex/8 + 1) * uint64(len(w.schema.Columns))

	// If current capacity is insufficient, grow the bitmap
	if uint64(len(w.nullBitmap)) < requiredSize {
		newBitmap := make([]byte, requiredSize*2) // Double the required size for future growth
		copy(newBitmap, w.nullBitmap)
		w.nullBitmap = newBitmap
	}
}

func (w *Writer) WriteRow(values []string) error {
	if len(values) != len(w.schema.Columns) {
		return errors.New("value count does not match schema")
	}

	// Calculate required bitmap size for this row
	newBitmapSize := ((w.rowCount+1)*uint64(len(w.schema.Columns)) + 7) / 8
	if newBitmapSize > uint64(len(w.nullBitmap)) {
		// Grow bitmap
		newBitmap := make([]byte, newBitmapSize*2) // Double for future growth
		copy(newBitmap, w.nullBitmap)
		w.nullBitmap = newBitmap
	}
	w.bitmapSize = newBitmapSize

	if w.rowCount%500000 == 0 {
		fmt.Printf("Writing row %d...\n", w.rowCount)
	}

	for i, val := range values {
		col := w.columns[i]
		if w.schema.Columns[i].Nullable {
			if err := w.appendNullableValue(col, val); err != nil {
				return err
			}
		} else {
			if isNull(val) {
				return fmt.Errorf("null value not allowed for non-nullable column %s", col.Metadata.Name)
			}
			if err := w.appendValue(col, val); err != nil {
				return err
			}
		}
	}
	w.rowCount++
	return nil
}
func (w *Writer) appendValue(col *ColumnBlock, value string) error {
	var buf [8]byte
	switch col.Metadata.Type {
	case types.Int32Type:
		val, err := parseInt32(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(buf[:4], uint32(val))
		col.Data = append(col.Data, buf[:4]...)

	case types.Int64Type:
		val, err := parseInt64(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(buf[:], uint64(val))
		col.Data = append(col.Data, buf[:]...)

	case types.Float32Type:
		val, err := parseFloat32(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(buf[:4], float32ToBits(val))
		col.Data = append(col.Data, buf[:4]...)

	case types.Float64Type:
		val, err := parseFloat64(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(buf[:], float64ToBits(val))
		col.Data = append(col.Data, buf[:]...)

	case types.StringType:
		col.Data = append(col.Data, []byte(value)...)

	case types.BooleanType:
		val, err := parseBoolean(value)
		if err != nil {
			return err
		}
		if val {
			col.Data = append(col.Data, byte(1))
		} else {
			col.Data = append(col.Data, byte(0))
		}

	case types.DateType:
		val, err := parseDate(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(buf[:], uint64(val.Unix()))
		col.Data = append(col.Data, buf[:]...)

	case types.TimestampType:
		val, err := parseTimestamp(value)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(buf[:], uint64(val.Unix()))
		col.Data = append(col.Data, buf[:]...)

	default:
		return errors.New("unsupported type")
	}
	return nil
}

func (w *Writer) getCompressor(colType types.DataType) (compression.Compressor, error) {
	switch colType {
	case types.TimestampType, types.DateType:
		compressor, err := compression.GetCompressor("temporal")
		if err != nil {
			return compression.GetCompressor("snappy")
		}
		return compressor, nil
	case types.Int32Type, types.Int64Type:
		compressor, err := compression.GetCompressor("delta")
		if err != nil {
			return compression.GetCompressor("snappy")
		}
		return compressor, nil
	case types.BooleanType:
		compressor, err := compression.GetCompressor("boolean")
		if err != nil {
			return compression.GetCompressor("snappy")
		}
		return compressor, nil
	default:
		return compression.GetCompressor("snappy")
	}
}

func (w *Writer) Close() error {
	fmt.Printf("Starting to write file with %d columns and %d rows\n", len(w.schema.Columns), w.rowCount)

	now := time.Now().Unix()
	header := encoding.FileHeader{
		Magic:         encoding.MagicNumber,
		Version:       encoding.Version,
		RowCount:      w.rowCount,
		ColumnCount:   uint32(len(w.schema.Columns)),
		Created:       now,
		Modified:      now,
		NullBitmapLen: uint32(w.bitmapSize),
	}

	schemaJson, err := json.Marshal(w.schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %v", err)
	}
	header.SchemaLen = uint32(len(schemaJson))

	if err := encoding.WriteHeader(w.w, header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	fmt.Printf("Writing schema JSON (length=%d)\n", len(schemaJson))
	if _, err := w.w.Write(schemaJson); err != nil {
		return fmt.Errorf("failed to write schema: %v", err)
	}

	fmt.Printf("Writing null bitmap (length=%d)\n", w.bitmapSize)
	if _, err := w.w.Write(w.nullBitmap[:w.bitmapSize]); err != nil {
		return fmt.Errorf("failed to write null bitmap: %v", err)
	}

	// Start metadata section after header + schema + nullBitmap
	currentOffset := int64(binary.Size(header) + len(schemaJson) + len(w.nullBitmap))
	metadataSize := 8 + 4 + 8 + 8 + 8 + 8
	for _, col := range w.columns {
		currentOffset += int64(metadataSize + len(col.Metadata.Name))
	}

	// Pre-compress all columns and store results
	type compressedColumn struct {
		data []byte
		size int64
	}
	compressedData := make(map[string]compressedColumn)

	for _, col := range w.columns {
		compressor, err := w.getCompressor(col.Metadata.Type)
		if err != nil {
			return fmt.Errorf("failed to get compressor for column %s: %v", col.Metadata.Name, err)
		}

		compressed, err := compressor.Compress(col.Data)
		if err != nil {
			return fmt.Errorf("compression failed for column %s: %v", col.Metadata.Name, err)
		}

		compressedData[col.Metadata.Name] = compressedColumn{
			data: compressed,
			size: int64(len(compressed)),
		}
	}

	// Write metadata
	for _, col := range w.columns {
		compressed := compressedData[col.Metadata.Name]

		col.Metadata.Offset = currentOffset
		col.Metadata.CompressedSize = compressed.size
		col.Metadata.Length = int64(len(col.Data))

		if err := binary.Write(w.w, binary.BigEndian, uint8(col.Metadata.Type)); err != nil {
			return err
		}

		nameBytes := []byte(col.Metadata.Name)
		if err := binary.Write(w.w, binary.BigEndian, uint32(len(nameBytes))); err != nil {
			return err
		}
		if _, err := w.w.Write(nameBytes); err != nil {
			return err
		}

		if err := binary.Write(w.w, binary.BigEndian, col.Metadata.Offset); err != nil {
			return err
		}
		if err := binary.Write(w.w, binary.BigEndian, col.Metadata.Length); err != nil {
			return err
		}
		if err := binary.Write(w.w, binary.BigEndian, col.Metadata.CompressedSize); err != nil {
			return err
		}
		if err := binary.Write(w.w, binary.BigEndian, col.Metadata.NullCount); err != nil {
			return err
		}

		currentOffset += compressed.size
	}

	// Write compressed data
	for _, col := range w.columns {
		compressed := compressedData[col.Metadata.Name]
		if _, err := w.w.Write(compressed.data); err != nil {
			return err
		}
	}

	return nil
}

func parseInt32(s string) (int32, error) {
	i64, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid int32 value: %s", s)
	}
	return int32(i64), nil
}

func parseInt64(s string) (int64, error) {
	i64, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid int64 value: %s", s)
	}
	return i64, nil
}

func parseFloat32(s string) (float32, error) {
	f64, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid float32 value: %s", s)
	}
	return float32(f64), nil
}

func parseFloat64(s string) (float64, error) {
	f64, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float64 value: %s", s)
	}
	return f64, nil
}

func parseBoolean(s string) (bool, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "true", "t", "1", "yes", "y":
		return true, nil
	case "false", "f", "0", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", s)
	}
}

func parseTimestamp(s string) (time.Time, error) {
	// Try common formats
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC822,
		time.RFC1123,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	// Try Unix timestamp
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(i, 0), nil
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", s)
}

func parseDate(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
		"January 2, 2006",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}

	if t, err := parseTimestamp(s); err == nil {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}

	return time.Time{}, fmt.Errorf("invalid date format: %s", s)
}

func float32ToBits(f float32) uint32 {
	return math.Float32bits(f)
}

func float64ToBits(f float64) uint64 {
	return math.Float64bits(f)
}

func (w *Writer) appendNullableValue(col *ColumnBlock, value string) error {
	colIndex := 0
	for i, c := range w.schema.Columns {
		if c.Name == col.Metadata.Name {
			colIndex = i
			break
		}
	}

	byteIndex := (w.rowCount*uint64(len(w.schema.Columns)) + uint64(colIndex)) / 8
	bitIndex := (w.rowCount*uint64(len(w.schema.Columns)) + uint64(colIndex)) % 8

	if isNull(value) {
		w.nullBitmap[byteIndex] |= (1 << bitIndex)
		return w.appendDefaultValue(col)
	}

	w.nullBitmap[byteIndex] &^= (1 << bitIndex)
	return w.appendValue(col, value)
}

func isNull(value string) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	return v == "" || v == "null" || v == "na" || v == "n/a"
}

func (w *Writer) appendDefaultValue(col *ColumnBlock) error {
	switch col.Metadata.Type {
	case types.Int32Type:
		return w.appendValue(col, "0")
	case types.Int64Type:
		return w.appendValue(col, "0")
	case types.Float32Type:
		return w.appendValue(col, "0.0")
	case types.Float64Type:
		return w.appendValue(col, "0.0")
	case types.StringType:
		return w.appendValue(col, "")
	case types.BooleanType:
		col.Data = append(col.Data, byte(0))
		return nil
	case types.DateType, types.TimestampType:
		return w.appendValue(col, "1970-01-01")
	default:
		return fmt.Errorf("unsupported type for null value")
	}
}
