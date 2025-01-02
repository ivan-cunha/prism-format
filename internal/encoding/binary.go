package encoding

import (
	"encoding/binary"
	"fmt"
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
	Created     int64
	Modified    int64
}

func WriteHeader(w io.Writer, header FileHeader) error {
	fmt.Printf("Writing header: Magic=%x, Version=%d, SchemaLen=%d, RowCount=%d, ColumnCount=%d\n",
		header.Magic, header.Version, header.SchemaLen, header.RowCount, header.ColumnCount)

	if err := binary.Write(w, binary.BigEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	return nil
}

func ReadHeader(r io.Reader) (FileHeader, error) {
	var header FileHeader
	err := binary.Read(r, binary.BigEndian, &header)
	if err != nil {
		return header, fmt.Errorf("failed to read header: %v", err)
	}

	fmt.Printf("Read header: Magic=%x, Version=%d, SchemaLen=%d, RowCount=%d, ColumnCount=%d\n",
		header.Magic, header.Version, header.SchemaLen, header.RowCount, header.ColumnCount)

	if header.Magic != MagicNumber {
		return header, fmt.Errorf("invalid magic number: expected %x, got %x", MagicNumber, header.Magic)
	}

	return header, nil
}
