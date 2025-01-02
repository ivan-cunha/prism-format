package compression

import (
	"errors"
	"sync"
)

var (
	ErrCompressorNotFound = errors.New("compressor not found")
	compressors           = make(map[string]Compressor)
	compressorsMu         sync.RWMutex
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

func Compress(data []byte) ([]byte, error) {
	c, err := GetCompressor("snappy") // Default compressor
	if err != nil {
		return nil, err
	}
	return c.Compress(data)
}

func Decompress(data []byte) ([]byte, error) {
	c, err := GetCompressor("snappy")
	if err != nil {
		return nil, err
	}
	return c.Decompress(data)
}
