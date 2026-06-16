# tinyparquet

**Version:** v0.1.0-beta

A zero-dependency, header-only C++17 Parquet reader. Designed to memory-map and decode highly-compressed Parquet files directly into memory without requiring the Apache Thrift compiler, Apache Arrow, Boost, zlib, Snappy, or any external compression libraries.

## The Parquet Challenge
Parquet is an incredibly complex columnar data format built for big data ecosystems. It requires navigating three primary layers of complexity:
1. **Metadata Serialization:** Parquet schemas and structural metadata are written at the end of the file using Apache Thrift's `TCompactProtocol`.
2. **Data Page Compression:** Column chunks are broken down into pages that are usually compressed using algorithms like Snappy, GZIP, LZ4, or Zstandard.
3. **Values Encoding:** The actual scalar values are tightly packed using Dictionary Encoding, Run-Length Encoding (RLE), Bit-Packing, or Delta Encoding.

**`tinyparquet` implements a native decoder for all three of these layers without a single external dependency.**

## Supported Formats

### Compression Codecs
Parquet files support many compression algorithms. `tinyparquet` implements several of these natively in C++ header-only form:
- [x] **UNCOMPRESSED**
- [x] **SNAPPY** (Native zero-dependency decompressor included)
- [x] **LZ4_RAW** (Native zero-dependency decompressor included)
- [x] **GZIP** (via bundled `third_party/miniz`)
- [x] **ZSTD** (via bundled `third_party/zstd`)
- [ ] BROTLI (Planned)

### Data Encodings
- [x] **PLAIN** (Raw values)
- [x] **PLAIN_DICTIONARY** (V1 Dictionary Pages)
- [x] **RLE_DICTIONARY** (V2 Dictionary Encoding)
- [x] **RLE / BIT_PACKED** (For Definition / Repetition levels)
- [ ] DELTA_BINARY_PACKED (Planned)
- [ ] DELTA_BYTE_ARRAY (Planned)

## Architecture
- **Header-Only:** Drop `tinyparquet.hpp` into your project.
- **Zero-Dependency:** No external libraries required. The custom `decompress.h` implements Snappy and LZ4 from scratch.
- **Zero-Copy Architecture:** Uses POSIX `mmap` to read binary data at RAM speed without buffering entire files into memory.
- **Custom Thrift Decoder:** Implements a lightweight `TCompactProtocol` decoder to parse Parquet FileMetaData without compiling Thrift structs.

## Usage

Include the single header file in your C++ project:

```cpp
#include "tinyparquet.hpp"
#include <iostream>
#include <vector>

int main() {
    try {
        // Initialize the reader
        tinyparquet::Reader reader("testing/alltypes_plain.snappy.parquet");
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

If you wish to enable GZIP and ZSTD decompression, define the compiler flags and compile the provided `third_party` sources:

```bash
gcc -c third_party/miniz/miniz.c -o miniz.o
gcc -c third_party/zstd/zstd.c -o zstd.o
g++ -std=c++17 -Ithird_party/miniz -Ithird_party/zstd -DTINYPARQUET_ENABLE_GZIP -DTINYPARQUET_ENABLE_ZSTD main.cpp miniz.o zstd.o -o app
```

## Development & Testing

Test files are located in the `testing/` directory. These are scraped from the official `apache/parquet-testing` repository to ensure compliance with the spec.

To regenerate `tinyparquet.hpp` from the `src/` directory after making changes:
```bash
python3 amalgamate.py
```
