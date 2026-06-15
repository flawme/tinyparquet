#pragma once
#include "common.h"
#include "mmap.h"
#include "metadata.h"
#include <cstring>

namespace tinyparquet {

class Reader {
public:
    explicit Reader(const std::string& path) : mmap_(path) {
        if (mmap_.size() < 8) throw ParquetException("File too small to be Parquet");
        
        const uint8_t* data = mmap_.data();
        size_t size = mmap_.size();
        
        // Check magic bytes
        if (std::memcmp(data, "PAR1", 4) != 0 || std::memcmp(data + size - 4, "PAR1", 4) != 0) {
            throw ParquetException("Invalid Parquet magic bytes");
        }
        
        // Read footer length
        uint32_t footer_len;
        std::memcpy(&footer_len, data + size - 8, 4);
        
        if (footer_len > size - 8) {
            throw ParquetException("Invalid footer length");
        }
        
        // Decode FileMetaData
        thrift::CompactDecoder decoder(data + size - 8 - footer_len, footer_len);
        metadata_.Parse(decoder);
    }

    const FileMetaData& GetMetaData() const { return metadata_; }

    ColumnReader GetColumnReader(const std::string& name) const {
        if (metadata_.row_groups.empty()) throw ParquetException("No row groups");
        for (const auto& chunk : metadata_.row_groups[0].columns) {
            if (!chunk.meta_data.path_in_schema.empty() && chunk.meta_data.path_in_schema[0] == name) {
                return ColumnReader(chunk, mmap_.data(), mmap_.size());
            }
        }
        throw ParquetException("Column not found");
    }

private:
    MemoryMappedFile mmap_;
    FileMetaData metadata_;
};

} // namespace tinyparquet
