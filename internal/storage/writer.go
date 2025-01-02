package storage

import (
	"io"

	"github.com/ivan-cunha/prism-format/internal/encoding"
	"github.com/ivan-cunha/prism-format/pkg/types"
)

type Writer struct {
	w       io.Writer
	schema  types.Schema
	columns []*ColumnBlock
}

func NewWriter(w io.Writer, schema types.Schema) *Writer {
	columns := make([]*ColumnBlock, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = NewColumnBlock(col.Name, col.Type)
	}

	return &Writer{
		w:       w,
		schema:  schema,
		columns: columns,
	}
}

func (w *Writer) WriteHeader() error {
	header := encoding.FileHeader{
		Magic:       encoding.MagicNumber,
		Version:     encoding.Version,
		ColumnCount: uint32(len(w.schema.Columns)),
	}

	return encoding.WriteHeader(w.w, header)
}
