package storage

import (
	"github.com/ivan-cunha/prism-format/pkg/types"
)

type ColumnMetadata struct {
	Type           types.DataType
	Name           string
	Offset         int64
	Length         int64
	CompressedSize int64
	NullCount      int64
	// Remove MinValue and MaxValue as they're interface{} types
	// We'll handle statistics separately if needed
}

type ColumnBlock struct {
	Metadata ColumnMetadata
	Data     []byte
}

func NewColumnBlock(name string, dtype types.DataType) *ColumnBlock {
	return &ColumnBlock{
		Metadata: ColumnMetadata{
			Type: dtype,
			Name: name,
		},
		Data: make([]byte, 0),
	}
}
