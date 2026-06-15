# tinyparquet

Version: v0.1.0-beta

A zero-dependency, header-only C++17 Parquet reader. Designed to memory-map and decode uncompressed Parquet files directly into memory without requiring the Apache Thrift compiler, Apache Arrow, or any external compression libraries.

## Status

Extensive testing is still required. This is a beta release supporting a subset of the Parquet specification.

## Features
- **Header-Only:** Drop `tinyparquet.hpp` into your project.
- **Zero-Dependency:** No external libraries required (no Thrift, Arrow, Boost, zlib, Snappy, etc.).
- **Zero-Copy Architecture:** Uses POSIX `mmap` to read binary data at RAM speed.
- **Custom Thrift Decoder:** Implements a lightweight `TCompactProtocol` decoder to parse Parquet metadata (FileMetaData, Schema, RowGroups, PageHeaders).
- **Data Decoding:** Supports reading uncompressed `PLAIN`, `RLE`, and `PLAIN_DICTIONARY` encoded pages.

## Usage

Include the single header file in your C++ project:

```cpp
#include "tinyparquet.hpp"
#include <iostream>
#include <vector>

int main() {
    try {
        // Initialize the reader
        tinyparquet::Reader reader("data.parquet");
        auto metadata = reader.GetMetaData();
        
        std::cout << "Rows: " << metadata.num_rows << "\n";
        
        // Extract values from an integer column
        auto int_reader = reader.GetColumnReader("int_col");
        std::vector<int32_t> int_values;
        int_reader.ReadAllInt32(int_values);
        
        for (auto v : int_values) {
            std::cout << v << " ";
        }
        std::cout << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
```

## Compilation

Since `tinyparquet` is a header-only library, no separate library compilation is required. Simply compile your code with C++17 support:

```bash
g++ -std=c++17 main.cpp -o app
```

## Limitations
- Compressed data pages (Snappy, GZIP, LZ4, ZSTD) are intentionally unsupported to maintain the zero-dependency requirement.
- Nested structures (Lists, Maps) require further implementation.
- This version targets flat uncompressed files utilizing dictionary or plain encoding.

## Build the Header

To regenerate `tinyparquet.hpp` from the source directory:
```bash
python3 amalgamate.py
```
