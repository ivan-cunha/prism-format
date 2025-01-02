package compression

import (
	"github.com/golang/snappy"
)

type SnappyCompressor struct{}

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func (c *SnappyCompressor) Name() string {
	return "snappy"
}

// Register snappy as default compressor
func init() {
	RegisterCompressor("snappy", NewSnappyCompressor())
}
