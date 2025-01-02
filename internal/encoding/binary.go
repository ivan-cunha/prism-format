package encoding

import (
	"encoding/binary"
	"io"
)

const (
	MagicNumber = 0x434F4C00 // "COL\0"
	Version     = 1
)

type FileHeader struct {
	Magic       uint32
	Version     uint16
	SchemaLen   uint32
	RowCount    uint64
	ColumnCount uint32
}

func WriteHeader(w io.Writer, header FileHeader) error {
	return binary.Write(w, binary.BigEndian, header)
}

func ReadHeader(r io.Reader) (FileHeader, error) {
	var header FileHeader
	err := binary.Read(r, binary.BigEndian, &header)
	return header, err
}
