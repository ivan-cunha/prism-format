# Prism Format

Prism Format is a project designed to explore and understand how column-based file formats work. While there are many production-ready columnar formats like Apache Parquet and Apache Arrow, this project serves as a learning tool to understand the core concepts and challenges involved in implementing a columnar storage format.

## What is a Column-Based Format?

Traditional row-based formats (like CSV) store data by grouping fields of each record together. In contrast, column-based formats store all the values of each column together. This approach offers several advantages:

- Better compression ratios since similar data is stored together
- Efficient querying of specific columns without reading unnecessary data
- Better CPU cache utilization when processing single columns

## How Prism Format Works

The format consists of several key components:

### File Structure
1. File Header
   - Magic number (COL\0) for file identification
   - Version information
   - Schema length
   - Row count and column count
   - Creation and modification timestamps

2. Schema Section
   - JSON-encoded schema describing all columns
   - Each column has a name, data type, and nullable flag

3. Column Metadata
   - Type information
   - Name
   - Offset and length information
   - Compression details

4. Column Data
   - Each column's data is stored separately
   - Data is compressed using either Snappy or Delta encoding
   - Support for various data types: Int32, Int64, Float32, Float64, String, Boolean, Date, Timestamp

### Features

- Schema-aware format with type safety
- Built-in compression support (Snappy and Delta encoding)
- Support for nullable fields
- Extensible compression system through a plugin architecture
- Simple CSV to Prism conversion utility

## Usage

### Converting CSV to Prism Format

```bash
./prism-format -cmd convert -input data.csv -output data.prsm
```

### Viewing File Information

```bash
./prism-format -cmd info -input data.prsm
```

This will display:
- File metadata (version, creation time, modification time)
- Schema information
- Column details (names, types, nullable status)

## Implementation Details

The project demonstrates several important concepts in file format design:

### Type System
The format supports multiple data types with careful handling of serialization and deserialization. This includes proper handling of endianness and type conversions.

### Compression
Two compression strategies are implemented:
- Snappy: General-purpose compression good for various data types
- Delta: Specialized encoding for sequential numeric data, particularly effective for timestamps and sorted numbers

### Memory Efficiency
The reader implements lazy loading of column data, only reading columns from disk when specifically requested. This helps manage memory usage when dealing with large files.

### Error Handling
The format includes various safety checks:
- Magic number validation
- Version compatibility checking
- Schema validation
- Type safety checks during reading and writing

## Building and Development

To build the project:

```bash
go build ./cmd/prism-format
```

The project uses Go modules for dependency management. Main dependencies include:
- github.com/golang/snappy for compression
- Standard Go libraries for binary encoding and JSON handling

## Project Structure

- `cmd/prism-format`: Main application entry point
- `internal/compression`: Compression implementations
- `internal/encoding`: Binary encoding utilities
- `internal/schema`: Schema management
- `internal/storage`: Core storage implementation
- `pkg/types`: Public data type definitions

## Limitations

This implementation has some limitations:
- No support for nested data structures
- Limited optimization for very large files
- Basic compression options
- No support for concurrent writes

These limitations make it unsuitable for production use but keep the code focused on demonstrating core concepts.
