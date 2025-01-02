package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ivan-cunha/prism-format/internal/schema"
	"github.com/ivan-cunha/prism-format/internal/storage"
	"github.com/ivan-cunha/prism-format/pkg/types"
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

	// Read CSV header
	reader := csv.NewReader(csvFile)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}

	// Create schema
	fileSchema := schema.New()
	for _, header := range headers {
		// Default to string type for all columns
		if err := fileSchema.AddColumn(header, types.StringType, true); err != nil {
			return fmt.Errorf("error adding column: %v", err)
		}
	}

	// Create output file
	outFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outFile.Close()

	// Create writer
	writer := storage.NewWriter(outFile, fileSchema.ToSchema())

	// Write data
	rows, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV data: %v", err)
	}

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			return fmt.Errorf("error writing row: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}

	fmt.Printf("Successfully converted %s to %s\n", input, output)
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
