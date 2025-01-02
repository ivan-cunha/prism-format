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
	MinValue       interface{}
	MaxValue       interface{}
	NullCount      int64
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
