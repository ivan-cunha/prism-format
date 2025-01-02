package schema

import (
	"fmt"

	"github.com/ivan-cunha/prism-format/pkg/types"
)

type ColumnSchema struct {
	Name         string         `json:"name"`
	Type         types.DataType `json:"type"`
	Nullable     bool           `json:"nullable"`
	DefaultValue interface{}    `json:"default_value,omitempty"`
	Description  string         `json:"description,omitempty"`
}

type FileSchema struct {
	Version     uint16         `json:"version"`
	Columns     []ColumnSchema `json:"columns"`
	Created     int64          `json:"created"`  // Unix timestamp
	Modified    int64          `json:"modified"` // Unix timestamp
	Description string         `json:"description,omitempty"`
}

func New() *FileSchema {
	return &FileSchema{
		Version: 1,
		Columns: make([]ColumnSchema, 0),
	}
}

func (s *FileSchema) AddColumn(name string, dataType types.DataType, nullable bool) error {
	// Validate column name uniqueness
	for _, col := range s.Columns {
		if col.Name == name {
			return fmt.Errorf("column name %s already exists", name)
		}
	}

	s.Columns = append(s.Columns, ColumnSchema{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
	})
	return nil
}

func (s *FileSchema) GetColumn(name string) (*ColumnSchema, error) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

func (s *FileSchema) Validate() error {
	if len(s.Columns) == 0 {
		return fmt.Errorf("schema must have at least one column")
	}

	// Check for duplicate column names
	names := make(map[string]bool)
	for _, col := range s.Columns {
		if names[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		names[col.Name] = true

		// Validate default values match column type
		if col.DefaultValue != nil {
			if err := validateDefaultValue(col.Type, col.DefaultValue); err != nil {
				return fmt.Errorf("invalid default value for column %s: %v", col.Name, err)
			}
		}
	}

	return nil
}

func validateDefaultValue(dt types.DataType, value interface{}) error {
	switch dt {
	case types.Int32Type:
		if _, ok := value.(int32); !ok {
			return fmt.Errorf("expected int32 value")
		}
	case types.Int64Type:
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected int64 value")
		}
	case types.Float32Type:
		if _, ok := value.(float32); !ok {
			return fmt.Errorf("expected float32 value")
		}
	case types.Float64Type:
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected float64 value")
		}
	case types.StringType:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value")
		}
	case types.BooleanType:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean value")
		}
	case types.TimestampType, types.DateType:
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected int64 timestamp value")
		}
	default:
		return fmt.Errorf("unsupported data type")
	}
	return nil
}

// Convert to/from types.Schema
func (s *FileSchema) ToSchema() types.Schema {
	columns := make([]types.Column, len(s.Columns))
	for i, col := range s.Columns {
		columns[i] = types.Column{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}
	return types.Schema{Columns: columns}
}

func FromSchema(schema types.Schema) *FileSchema {
	fs := New()
	for _, col := range schema.Columns {
		fs.AddColumn(col.Name, col.Type, col.Nullable)
	}
	return fs
}
