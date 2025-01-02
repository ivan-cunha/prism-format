package types

import "encoding/json"

type DataType uint8

const (
	Int32Type DataType = iota
	Int64Type
	Float32Type
	Float64Type
	StringType
	BooleanType
	TimestampType
	DateType
)

// Add String method for better debugging and logging
func (d DataType) String() string {
	return [...]string{
		"Int32", "Int64", "Float32", "Float64",
		"String", "Boolean", "Timestamp", "Date",
	}[d]
}

type Column struct {
	Name     string
	Type     DataType
	Nullable bool
	// Add useful fields for column metadata
	DefaultValue interface{} // For nullable columns
	Description  string      // Documentation
}

type Schema struct {
	Columns []Column
}

func (s Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Columns)
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &s.Columns)
}
