package compression

import (
	"errors"
	"sync"
)

var (
	ErrCompressorNotFound = errors.New("compressor not found")
	compressors           = make(map[string]Compressor)
	compressorsMu         sync.RWMutex
	ErrInvalidDataSize    = errors.New("invalid data size for compression")
	ErrInvalidFormat      = errors.New("invalid compressed data format")
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Name() string
}

func RegisterCompressor(name string, c Compressor) {
	compressorsMu.Lock()
	defer compressorsMu.Unlock()
	compressors[name] = c
}

func GetCompressor(name string) (Compressor, error) {
	compressorsMu.RLock()
	defer compressorsMu.RUnlock()

	if c, exists := compressors[name]; exists {
		return c, nil
	}
	return nil, ErrCompressorNotFound
}
