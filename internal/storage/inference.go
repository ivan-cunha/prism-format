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

type TypeInference struct {
	possibleTypes map[types.DataType]bool
	nullCount     int
	sampleCount   int
	nullable      bool
}

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
				types.StringType:    true,
			},
			nullCount:   0,
			sampleCount: 0,
			nullable:    false,
		}
	}

	// Read sample rows
	for i := 0; i < sampleSize; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
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

	columns := finalizeTypes(inferences, headers)
	dataTypes := make([]types.DataType, len(columns))
	for i, col := range columns {
		dataTypes[i] = col.Type
	}

	return dataTypes, nil
}

func InferColumnDetails(r io.ReadSeeker) ([]types.Column, error) {
	// This contains the full inference logic that returns Column structs
	// The implementation is the same as what we had in the previous InferColumnTypes
	// but keeps the Column struct return type
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %v", err)
	}

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
				types.StringType:    true,
			},
			nullCount:   0,
			sampleCount: 0,
			nullable:    false,
		}
	}

	for i := 0; i < sampleSize; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if strings.Contains(err.Error(), "quote") {
				reader.LazyQuotes = true
				continue
			}
			return nil, err
		}

		for j, value := range row {
			analyzeValue(&inferences[j], value)
		}
	}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return finalizeTypes(inferences, headers), nil
}

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

func InferColumnDetailsWithRetry(r io.ReadSeeker) ([]types.Column, error) {
	// First attempt with standard settings
	columns, err := InferColumnDetails(r)
	if err == nil {
		return columns, nil
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

	return InferColumnDetails(r)
}

func analyzeValue(inference *TypeInference, value string) {
	inference.sampleCount++
	value = strings.TrimSpace(value)

	// Check for null/empty values
	if value == "" || strings.ToLower(value) == "null" ||
		strings.ToLower(value) == "na" || strings.ToLower(value) == "n/a" {
		inference.nullCount++
		inference.nullable = true // Mark as nullable when we find a null
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

func finalizeTypes(inferences []TypeInference, headers []string) []types.Column {
	result := make([]types.Column, len(inferences))

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
		// Create the column with name from headers
		result[i] = types.Column{
			Name: headers[i],
			Type: types.StringType, // Default to string
		}

		// Select the most specific type that's still possible
		for _, t := range typeOrder {
			if inf.possibleTypes[t] {
				result[i].Type = t
				break
			}
		}

		result[i].Nullable = inf.nullable
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
		if isNull(convErr.Value) && !schema.Columns[convErr.ColumnIndex].Nullable {
			fmt.Printf("Warning: Found null value in non-nullable column '%s', converting to nullable\n",
				convErr.ColumnName)
			schema.Columns[convErr.ColumnIndex].Nullable = true
		} else {
			// If it's not a null value issue, fall back to string type
			fmt.Printf("Warning: Column '%s' contains mixed types, falling back to string type\n",
				convErr.ColumnName)
			schema.Columns[convErr.ColumnIndex].Type = types.StringType
		}

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
