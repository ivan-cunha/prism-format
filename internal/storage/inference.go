package storage

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/ivan-cunha/prism-format/pkg/types"
)

const sampleSize = 100

// TypeInference holds the state of type inference for a column
type TypeInference struct {
	possibleTypes map[types.DataType]bool
	nullCount     int
	sampleCount   int
}

// InferColumnTypes reads the CSV with proper configuration and determines types
func InferColumnTypes(r io.ReadSeeker) ([]types.DataType, error) {
	// Configure CSV reader with proper settings
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %v", err)
	}

	// Initialize type inference state for each column
	inferences := make([]TypeInference, len(headers))
	for i := range inferences {
		inferences[i] = TypeInference{
			possibleTypes: map[types.DataType]bool{
				types.BooleanType:   true,
				types.Int32Type:     true,
				types.Int64Type:     true,
				types.Float32Type:   true,
				types.Float64Type:   true,
				types.DateType:      true,
				types.TimestampType: true,
				types.StringType:    true, // Always possible as fallback
			},
			nullCount:   0,
			sampleCount: 0,
		}
	}

	// Read sample rows
	for i := 0; i < sampleSize; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If it's a quote-related error, try with more permissive settings
			if strings.Contains(err.Error(), "quote") {
				reader.LazyQuotes = true
				continue
			}
			return nil, err
		}

		// Analyze each column value
		for j, value := range row {
			analyzeValue(&inferences[j], value)
		}
	}

	// Reset file position
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return finalizeTypes(inferences), nil
}

// InferColumnTypesWithRetry attempts type inference with progressively more permissive settings
func InferColumnTypesWithRetry(r io.ReadSeeker) ([]types.DataType, error) {
	// First attempt with standard settings
	types, err := InferColumnTypes(r)
	if err == nil {
		return types, nil
	}

	// Reset position for retry
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Retry with more permissive CSV reader settings
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	return InferColumnTypes(r)
}

func analyzeValue(inference *TypeInference, value string) {
	inference.sampleCount++
	value = strings.TrimSpace(value)

	// Check for null/empty values
	if value == "" || strings.ToLower(value) == "null" || strings.ToLower(value) == "na" {
		inference.nullCount++
		return
	}

	// Test each type if it's still possible
	if inference.possibleTypes[types.BooleanType] {
		if _, err := parseBoolean(value); err != nil {
			inference.possibleTypes[types.BooleanType] = false
		}
	}

	if inference.possibleTypes[types.Int32Type] {
		if val, err := parseInt64(value); err != nil || val > math.MaxInt32 || val < math.MinInt32 {
			inference.possibleTypes[types.Int32Type] = false
		}
	}

	if inference.possibleTypes[types.Int64Type] {
		if _, err := parseInt64(value); err != nil {
			inference.possibleTypes[types.Int64Type] = false
		}
	}

	if inference.possibleTypes[types.Float32Type] {
		if val, err := parseFloat64(value); err != nil || val > math.MaxFloat32 || val < -math.MaxFloat32 {
			inference.possibleTypes[types.Float32Type] = false
		}
	}

	if inference.possibleTypes[types.Float64Type] {
		if _, err := parseFloat64(value); err != nil {
			inference.possibleTypes[types.Float64Type] = false
		}
	}

	if inference.possibleTypes[types.DateType] {
		if t, err := parseDate(value); err != nil {
			inference.possibleTypes[types.DateType] = false
		} else {
			hour, min, sec := t.Clock()
			if hour != 0 || min != 0 || sec != 0 {
				inference.possibleTypes[types.DateType] = false
			}
		}
	}

	if inference.possibleTypes[types.TimestampType] {
		if _, err := parseTimestamp(value); err != nil {
			inference.possibleTypes[types.TimestampType] = false
		}
	}
}

func finalizeTypes(inferences []TypeInference) []types.DataType {
	result := make([]types.DataType, len(inferences))

	typeOrder := []types.DataType{
		types.BooleanType,
		types.Int32Type,
		types.Int64Type,
		types.Float32Type,
		types.Float64Type,
		types.DateType,
		types.TimestampType,
		types.StringType,
	}

	for i, inf := range inferences {
		// Default to string type
		result[i] = types.StringType

		// Select the most specific type that's still possible
		for _, t := range typeOrder {
			if inf.possibleTypes[t] {
				result[i] = t
				break
			}
		}

		// If more than 90% of values are null, make it nullable
		nullRatio := float64(inf.nullCount) / float64(inf.sampleCount)
		if nullRatio > 0.9 {
			// You might want to add a SetNullable method to your schema
			// or handle this at a higher level
		}
	}

	return result
}

type ConversionError struct {
	ColumnIndex int
	ColumnName  string
	Value       string
	TargetType  types.DataType
	Row         int
}

func (e *ConversionError) Error() string {
	return fmt.Sprintf("conversion error at row %d, column '%s' (index %d): value '%s' cannot be converted to %s",
		e.Row, e.ColumnName, e.ColumnIndex, e.Value, e.TargetType)
}

// WriteResult represents the result of a write operation
type WriteResult struct {
	RowsProcessed int
	Error         error
}

// ConvertWithRetry handles the CSV conversion with automatic type fallback
func ConvertWithRetry(reader io.ReadSeeker, writer *Writer, schema types.Schema) error {
	for {
		result := writeWithTypeCheck(reader, writer, schema)

		if result.Error == nil {
			return nil
		}

		// Check if it's a conversion error
		convErr, ok := result.Error.(*ConversionError)
		if !ok {
			return result.Error
		}

		// Fall back to string type for the problematic column
		fmt.Printf("Warning: Column '%s' contains mixed types, falling back to string type\n",
			convErr.ColumnName)

		// Update schema for the problematic column
		schema.Columns[convErr.ColumnIndex].Type = types.StringType

		// Reset reader position
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to reset reader: %v", err)
		}

		// Create new writer with updated schema
		writer = NewWriter(writer.w, schema)
	}
}

func writeWithTypeCheck(reader io.ReadSeeker, writer *Writer, schema types.Schema) WriteResult {
	csvReader := csv.NewReader(reader)
	rowNum := 0

	// Skip header row
	_, err := csvReader.Read()
	if err != nil {
		return WriteResult{0, fmt.Errorf("error reading header: %v", err)}
	}

	for {
		// Read row
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return WriteResult{rowNum, fmt.Errorf("error reading row: %v", err)}
		}

		rowNum++

		// Validate and convert values
		for i, val := range row {
			col := schema.Columns[i]
			if err := validateTypeConversion(val, col.Type); err != nil {
				return WriteResult{
					RowsProcessed: rowNum,
					Error: &ConversionError{
						ColumnIndex: i,
						ColumnName:  col.Name,
						Value:       val,
						TargetType:  col.Type,
						Row:         rowNum,
					},
				}
			}
		}

		// Write row
		if err := writer.WriteRow(row); err != nil {
			return WriteResult{rowNum, fmt.Errorf("error writing row: %v", err)}
		}

		rowNum++
	}

	return WriteResult{rowNum, nil}
}

func validateTypeConversion(value string, dtype types.DataType) error {
	switch dtype {
	case types.Int32Type:
		if _, err := parseInt32(value); err != nil {
			return err
		}
	case types.Int64Type:
		if _, err := parseInt64(value); err != nil {
			return err
		}
	case types.Float32Type:
		if _, err := parseFloat32(value); err != nil {
			return err
		}
	case types.Float64Type:
		if _, err := parseFloat64(value); err != nil {
			return err
		}
	case types.BooleanType:
		if _, err := parseBoolean(value); err != nil {
			return err
		}
	case types.DateType:
		if _, err := parseDate(value); err != nil {
			return err
		}
	case types.TimestampType:
		if _, err := parseTimestamp(value); err != nil {
			return err
		}
	}
	return nil
}
