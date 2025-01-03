package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ivan-cunha/prism-format/internal/schema"
	"github.com/ivan-cunha/prism-format/internal/storage"
)

const (
	PrismExtension = ".prsm"
)

func main() {
	var (
		inputFile  string
		outputFile string
		command    string
	)

	flag.StringVar(&inputFile, "input", "", "Input file path")
	flag.StringVar(&outputFile, "output", "", "Output file path")
	flag.StringVar(&command, "cmd", "convert", "Command to execute (convert, info)")
	flag.Parse()

	switch command {
	case "convert":
		if err := convertCSVToPrism(inputFile, outputFile); err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}
	case "info":
		if err := showPrismInfo(inputFile); err != nil {
			fmt.Printf("Error reading file info: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

// ensurePrismExtension ensures the file has the .prsm extension
func ensurePrismExtension(filename string) string {
	if !strings.HasSuffix(strings.ToLower(filename), PrismExtension) {
		return filename + PrismExtension
	}
	return filename
}

func validatePrismFile(filename string) error {
	if !strings.HasSuffix(strings.ToLower(filename), PrismExtension) {
		return fmt.Errorf("invalid file extension: file must have %s extension", PrismExtension)
	}
	return nil
}

func convertCSVToPrism(input, output string) error {
	if input == "" || output == "" {
		return fmt.Errorf("input and output files are required")
	}

	// Ensure output has .prsm extension
	output = ensurePrismExtension(output)

	// Open CSV file
	csvFile, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("error opening CSV file: %v", err)
	}
	defer csvFile.Close()

	// Create CSV reader with proper configuration
	reader := csv.NewReader(csvFile)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}

	// Validate headers
	if err := validateHeaders(headers); err != nil {
		return fmt.Errorf("invalid headers: %v", err)
	}

	// Infer column types with retry logic
	columnTypes, err := storage.InferColumnTypesWithRetry(csvFile)
	if err != nil {
		return fmt.Errorf("error inferring column types: %v", err)
	}

	// Reset file position for reading data
	if _, err := csvFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("error resetting file position: %v", err)
	}

	// Create schema with inferred types
	fileSchema := schema.New()
	for i, header := range headers {
		if err := fileSchema.AddColumn(header, columnTypes[i], true); err != nil {
			return fmt.Errorf("error adding column: %v", err)
		}
	}

	// Create output file
	outFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outFile.Close()

	// Create writer and convert
	writer := storage.NewWriter(outFile, fileSchema.ToSchema())
	if err := storage.ConvertWithRetry(csvFile, writer, fileSchema.ToSchema()); err != nil {
		return fmt.Errorf("error during conversion: %v", err)
	}

	return writer.Close()
}

func validateHeaders(headers []string) error {
	seen := make(map[string]bool)
	for i, header := range headers {
		// Check for empty headers
		if strings.TrimSpace(header) == "" {
			return fmt.Errorf("empty header at position %d", i)
		}

		// Check for duplicate headers
		if seen[header] {
			return fmt.Errorf("duplicate header: %s", header)
		}
		seen[header] = true
	}
	return nil
}
func showPrismInfo(input string) error {
	// Validate file extension
	if err := validatePrismFile(input); err != nil {
		return err
	}

	file, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader, err := storage.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating reader: %v", err)
	}

	info, err := reader.GetFileInfo()
	if err != nil {
		return fmt.Errorf("error reading file info: %v", err)
	}

	fmt.Printf("File: %s\n", filepath.Base(input))
	fmt.Printf("Version: %d\n", info.Version)
	fmt.Printf("Created: %s\n", time.Unix(info.Created, 0))
	fmt.Printf("Modified: %s\n", time.Unix(info.Modified, 0))
	fmt.Printf("Columns: %d\n", len(info.Schema.Columns))

	fmt.Println("\nColumns:")
	for _, col := range info.Schema.Columns {
		fmt.Printf("- %s (%s, nullable: %v)\n",
			col.Name, col.Type.String(), col.Nullable)
	}

	return nil
}
