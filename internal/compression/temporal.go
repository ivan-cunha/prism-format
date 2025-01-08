package compression

import (
	"encoding/binary"
	"time"
)

type TemporalCompressor struct {
	referencePoint int64
}

func NewTemporalCompressor() *TemporalCompressor {
	return &TemporalCompressor{
		referencePoint: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}
}

func (c *TemporalCompressor) Name() string {
	return "temporal"
}

func (c *TemporalCompressor) Compress(data []byte) ([]byte, error) {
	// Each timestamp/date is 8 bytes
	if len(data)%8 != 0 {
		return nil, ErrInvalidDataSize
	}

	timestamps := make([]int64, len(data)/8)
	for i := range timestamps {
		timestamps[i] = int64(binary.BigEndian.Uint64(data[i*8 : (i+1)*8]))
	}

	// Find min timestamp to use as reference point
	if len(timestamps) > 0 {
		c.referencePoint = timestamps[0]
		for _, ts := range timestamps {
			if ts < c.referencePoint {
				c.referencePoint = ts
			}
		}
	}

	// Store reference point and offsets
	result := make([]byte, 8+len(timestamps)*4) // 8 for reference + 4 bytes per offset
	binary.BigEndian.PutUint64(result[0:8], uint64(c.referencePoint))

	// Store offsets using 4 bytes instead of 8
	for i, ts := range timestamps {
		offset := uint32(ts - c.referencePoint)
		binary.BigEndian.PutUint32(result[8+i*4:8+(i+1)*4], offset)
	}

	return result, nil
}

func (c *TemporalCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, ErrInvalidDataSize
	}

	// Read reference point
	referencePoint := int64(binary.BigEndian.Uint64(data[0:8]))
	offsetCount := (len(data) - 8) / 4

	result := make([]byte, offsetCount*8)

	// Reconstruct timestamps
	for i := 0; i < offsetCount; i++ {
		offset := binary.BigEndian.Uint32(data[8+i*4 : 8+(i+1)*4])
		timestamp := referencePoint + int64(offset)
		binary.BigEndian.PutUint64(result[i*8:(i+1)*8], uint64(timestamp))
	}

	return result, nil
}

func init() {
	RegisterCompressor("temporal", NewTemporalCompressor())
}
