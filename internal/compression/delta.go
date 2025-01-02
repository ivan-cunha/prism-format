package compression

import (
	"encoding/binary"
	"errors"
)

type DeltaCompressor struct{}

func NewDeltaCompressor() *DeltaCompressor {
	return &DeltaCompressor{}
}

func (c *DeltaCompressor) Name() string {
	return "delta"
}

func (c *DeltaCompressor) Compress(data []byte) ([]byte, error) {
	if len(data)%8 != 0 {
		return nil, errors.New("data length must be multiple of 8 bytes")
	}

	// Convert bytes to int64 for delta encoding
	nums := make([]int64, len(data)/8)
	for i := range nums {
		nums[i] = int64(binary.BigEndian.Uint64(data[i*8 : (i+1)*8]))
	}

	// Calculate deltas
	result := make([]byte, len(data))
	binary.BigEndian.PutUint64(result[0:8], uint64(nums[0])) // Store first value as-is

	for i := 1; i < len(nums); i++ {
		delta := nums[i] - nums[i-1]
		binary.BigEndian.PutUint64(result[i*8:(i+1)*8], uint64(delta))
	}

	return result, nil
}

func (c *DeltaCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data)%8 != 0 {
		return nil, errors.New("data length must be multiple of 8 bytes")
	}

	result := make([]byte, len(data))

	// Copy first value as-is
	copy(result[0:8], data[0:8])
	prev := int64(binary.BigEndian.Uint64(data[0:8]))

	// Reconstruct values from deltas
	for i := 1; i < len(data)/8; i++ {
		delta := int64(binary.BigEndian.Uint64(data[i*8 : (i+1)*8]))
		value := prev + delta
		binary.BigEndian.PutUint64(result[i*8:(i+1)*8], uint64(value))
		prev = value
	}

	return result, nil
}

func init() {
	RegisterCompressor("delta", NewDeltaCompressor())
}
