package compression

import (
	"encoding/binary"
	"errors"
)

// BooleanCompressor implements run-length encoding (RLE) for boolean values
type BooleanCompressor struct{}

func NewBooleanCompressor() *BooleanCompressor {
	return &BooleanCompressor{}
}

func (c *BooleanCompressor) Name() string {
	return "boolean"
}

// Compress implements RLE compression for boolean data
// Format: Each run is encoded as a uint32 count followed by a single byte (0 or 1)
func (c *BooleanCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Pre-allocate result buffer (worst case: alternating values)
	result := make([]byte, 0, len(data)*5) // 4 bytes for count + 1 byte for value

	// Track current run
	currentVal := (data[0] & 1) == 1
	runLength := uint32(1)

	// Process all bytes
	for i := 1; i < len(data); i++ {
		val := (data[i] & 1) == 1

		if val == currentVal && runLength < ^uint32(0) {
			// Continue current run if not at max length
			runLength++
		} else {
			// End current run and start new one
			countBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(countBuf, runLength)
			result = append(result, countBuf...)
			if currentVal {
				result = append(result, 1)
			} else {
				result = append(result, 0)
			}

			currentVal = val
			runLength = 1
		}
	}

	// Add final run
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, runLength)
	result = append(result, countBuf...)
	if currentVal {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}

	return result, nil
}

// Decompress decodes RLE-compressed boolean data
func (c *BooleanCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	if len(data) < 5 { // Need at least one complete run (4 bytes count + 1 byte value)
		return nil, errors.New("invalid compressed data: too short")
	}

	// First pass: calculate total decompressed size
	totalSize := uint32(0)
	for i := 0; i < len(data); i += 5 {
		if i+5 > len(data) {
			return nil, errors.New("invalid compressed data: incomplete run")
		}
		runLength := binary.BigEndian.Uint32(data[i:])
		totalSize += runLength
	}

	// Allocate result buffer
	result := make([]byte, totalSize)

	// Second pass: decompress runs
	pos := uint32(0)
	for i := 0; i < len(data); i += 5 {
		runLength := binary.BigEndian.Uint32(data[i:])
		value := data[i+4]

		// Validate run
		if pos+runLength > totalSize {
			return nil, errors.New("invalid compressed data: run exceeds expected size")
		}

		// Write run
		for j := uint32(0); j < runLength; j++ {
			result[pos+j] = value
		}
		pos += runLength
	}

	if pos != totalSize {
		return nil, errors.New("invalid compressed data: size mismatch")
	}

	return result, nil
}

func init() {
	RegisterCompressor("boolean", NewBooleanCompressor())
}
