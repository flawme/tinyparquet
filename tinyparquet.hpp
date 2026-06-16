// TinyParquet - Single Header Parquet Reader
// Generated on 2026-06-16 20:45:23

#pragma once

#include <brotli/decode.h>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <miniz.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <zstd.h>

namespace tinyparquet {

// --- src/common.h ---


// Exception class for Parquet parsing errors
class ParquetException : public std::runtime_error {
public:
    explicit ParquetException(const std::string& msg) : std::runtime_error(msg) {}
};


// --- src/mmap.h ---


class MemoryMappedFile {
public:
    explicit MemoryMappedFile(const std::string& path) {
        fd_ = open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw ParquetException("Failed to open file: " + path);
        }

        struct stat st;
        if (fstat(fd_, &st) < 0) {
            close(fd_);
            throw ParquetException("Failed to stat file: " + path);
        }
        size_ = st.st_size;

        if (size_ > 0) {
            data_ = static_cast<const uint8_t*>(mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0));
            if (data_ == MAP_FAILED) {
                close(fd_);
                throw ParquetException("Failed to mmap file: " + path);
            }
        } else {
            data_ = nullptr;
        }
    }

    ~MemoryMappedFile() {
        if (data_ && data_ != MAP_FAILED) {
            munmap(const_cast<uint8_t*>(data_), size_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }

private:
    int fd_;
    size_t size_;
    const uint8_t* data_;
};


// --- src/thrift.h ---

namespace thrift {

enum class TType : uint8_t {
    STOP = 0,
    BOOLEAN_TRUE = 1,
    BOOLEAN_FALSE = 2,
    BYTE = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    DOUBLE = 7,
    BINARY = 8,
    LIST = 9,
    SET = 10,
    MAP = 11,
    STRUCT = 12,
    UUID = 13
};

class CompactDecoder {
public:
    CompactDecoder(const uint8_t* data, size_t size) : start_(data), ptr_(data), end_(data + size), last_field_id_(0) {}

    size_t GetBytesRead() const { return ptr_ - start_; }

    uint64_t ReadVarInt() {
        uint64_t result = 0;
        uint32_t shift = 0;
        while (ptr_ < end_) {
            uint8_t byte = *ptr_++;
            result |= static_cast<uint64_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) return result;
            shift += 7;
        }
        throw ParquetException("Unexpected end of data in VarInt");
    }

    int64_t ReadZigZag() {
        uint64_t v = ReadVarInt();
        return (v >> 1) ^ -(v & 1);
    }

    std::string ReadBinary() {
        uint64_t len = ReadVarInt();
        if (ptr_ + len > end_) throw ParquetException("Binary length exceeds buffer");
        std::string s(reinterpret_cast<const char*>(ptr_), len);
        ptr_ += len;
        return s;
    }

    bool ReadFieldBegin(TType& type, int16_t& id) {
        if (ptr_ >= end_) return false;
        uint8_t byte = *ptr_++;
        type = static_cast<TType>(byte & 0x0f);
        if (type == TType::STOP) return false;

        int16_t modifier = (byte & 0xf0) >> 4;
        if (modifier == 0) {
            id = ReadZigZag();
        } else {
            id = last_field_id_ + modifier;
        }
        last_field_id_ = id;
        return true;
    }

    uint32_t ReadListBegin(TType& elem_type) {
        uint8_t b = *ptr_++;
        uint32_t size = b >> 4;
        elem_type = static_cast<TType>(b & 0x0f);
        if (size == 15) size = ReadVarInt();
        return size;
    }

    void Skip(TType type) {
        switch (type) {
            case TType::BOOLEAN_TRUE:
            case TType::BOOLEAN_FALSE: break;
            case TType::BYTE: ptr_++; break;
            case TType::I16:
            case TType::I32:
            case TType::I64: ReadVarInt(); break;
            case TType::DOUBLE: ptr_ += 8; break;
            case TType::BINARY: ptr_ += ReadVarInt(); break;
            case TType::LIST:
            case TType::SET: {
                uint8_t b = *ptr_++;
                uint32_t size = b >> 4;
                TType elem_type = static_cast<TType>(b & 0x0f);
                if (size == 15) size = ReadVarInt();
                for (uint32_t i = 0; i < size; i++) Skip(elem_type);
                break;
            }
            case TType::MAP: {
                uint32_t size = ReadVarInt();
                if (size > 0) {
                    uint8_t b = *ptr_++;
                    TType kt = static_cast<TType>(b >> 4);
                    TType vt = static_cast<TType>(b & 0x0f);
                    for (uint32_t i = 0; i < size; i++) {
                        Skip(kt);
                        Skip(vt);
                    }
                }
                break;
            }
            case TType::STRUCT: {
                TType ftype;
                int16_t fid;
                int16_t old_id = last_field_id_;
                last_field_id_ = 0;
                while (ReadFieldBegin(ftype, fid)) {
                    Skip(ftype);
                }
                last_field_id_ = old_id;
                break;
            }
            default:
                throw ParquetException("Unknown Thrift type during skip");
        }
    }

    int16_t last_field_id_;

private:
    const uint8_t* start_;
    const uint8_t* ptr_;
    const uint8_t* end_;
};

} // namespace thrift

// --- src/metadata.h ---


enum class Type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
};

enum class CompressionCodec {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
    LZ4_RAW = 7
};

struct SchemaElement {
    Type type;
    int32_t type_length = 0;
    int32_t repetition_type = 0; // 0=REQUIRED, 1=OPTIONAL, 2=REPEATED
    std::string name;
    int32_t num_children = 0;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) type = static_cast<Type>(decoder.ReadZigZag());
            else if (fid == 2 && ftype == thrift::TType::I32) type_length = decoder.ReadZigZag();
            else if (fid == 3 && ftype == thrift::TType::I32) repetition_type = decoder.ReadZigZag();
            else if (fid == 4 && ftype == thrift::TType::BINARY) name = decoder.ReadBinary();
            else if (fid == 5 && ftype == thrift::TType::I32) num_children = decoder.ReadZigZag();
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct ColumnMetaData {
    Type type;
    std::vector<std::string> path_in_schema;
    CompressionCodec codec;
    int64_t num_values;
    int64_t total_uncompressed_size;
    int64_t total_compressed_size;
    int64_t data_page_offset;
    int64_t dictionary_page_offset = 0;
    bool has_dictionary_page = false;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) type = static_cast<Type>(decoder.ReadZigZag());
            else if (fid == 2 && ftype == thrift::TType::LIST) {
                thrift::TType elem_type;
                uint32_t size = decoder.ReadListBegin(elem_type);
                for (uint32_t i = 0; i < size; ++i) {
                    decoder.ReadZigZag(); // skip encodings
                }
            }
            else if (fid == 3 && ftype == thrift::TType::LIST) {
                thrift::TType elem_type;
                uint32_t size = decoder.ReadListBegin(elem_type);
                for (uint32_t i = 0; i < size; ++i) {
                    path_in_schema.push_back(decoder.ReadBinary());
                }
            }
            else if (fid == 4 && ftype == thrift::TType::I32) codec = static_cast<CompressionCodec>(decoder.ReadZigZag());
            else if (fid == 5 && ftype == thrift::TType::I64) num_values = decoder.ReadZigZag();
            else if (fid == 6 && ftype == thrift::TType::I64) total_uncompressed_size = decoder.ReadZigZag();
            else if (fid == 7 && ftype == thrift::TType::I64) total_compressed_size = decoder.ReadZigZag();
            else if (fid == 9 && ftype == thrift::TType::I64) data_page_offset = decoder.ReadZigZag();
            else if (fid == 11 && ftype == thrift::TType::I64) {
                dictionary_page_offset = decoder.ReadZigZag();
                has_dictionary_page = true;
            }
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct ColumnChunk {
    int64_t file_offset;
    ColumnMetaData meta_data;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 2 && ftype == thrift::TType::I64) file_offset = decoder.ReadZigZag();
            else if (fid == 3 && ftype == thrift::TType::STRUCT) meta_data.Parse(decoder);
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct RowGroup {
    std::vector<ColumnChunk> columns;
    int64_t total_byte_size;
    int64_t num_rows;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::LIST) {
                thrift::TType elem_type;
                uint32_t size = decoder.ReadListBegin(elem_type);
                for (uint32_t i = 0; i < size; ++i) {
                    ColumnChunk chunk;
                    chunk.Parse(decoder);
                    columns.push_back(chunk);
                }
            }
            else if (fid == 2 && ftype == thrift::TType::I64) total_byte_size = decoder.ReadZigZag();
            else if (fid == 3 && ftype == thrift::TType::I64) num_rows = decoder.ReadZigZag();
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct FileMetaData {
    int32_t version;
    std::vector<SchemaElement> schema;
    int64_t num_rows;
    std::vector<RowGroup> row_groups;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) version = decoder.ReadZigZag();
            else if (fid == 2 && ftype == thrift::TType::LIST) {
                thrift::TType elem_type;
                uint32_t size = decoder.ReadListBegin(elem_type);
                for (uint32_t i = 0; i < size; ++i) {
                    SchemaElement elem;
                    elem.Parse(decoder);
                    schema.push_back(elem);
                }
            }
            else if (fid == 3 && ftype == thrift::TType::I64) num_rows = decoder.ReadZigZag();
            else if (fid == 4 && ftype == thrift::TType::LIST) {
                thrift::TType elem_type;
                uint32_t size = decoder.ReadListBegin(elem_type);
                for (uint32_t i = 0; i < size; ++i) {
                    RowGroup rg;
                    rg.Parse(decoder);
                    row_groups.push_back(rg);
                }
            }
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

enum class PageType {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3
};

struct DataPageHeader {
    int32_t num_values = 0;
    int32_t encoding = 0;
    int32_t definition_level_encoding = 0;
    int32_t repetition_level_encoding = 0;

    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) num_values = decoder.ReadZigZag();
            else if (fid == 2 && ftype == thrift::TType::I32) encoding = decoder.ReadZigZag();
            else if (fid == 3 && ftype == thrift::TType::I32) definition_level_encoding = decoder.ReadZigZag();
            else if (fid == 4 && ftype == thrift::TType::I32) repetition_level_encoding = decoder.ReadZigZag();
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct DictionaryPageHeader {
    int32_t num_values = 0;
    int32_t encoding = 0;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) num_values = decoder.ReadZigZag();
            else if (fid == 2 && ftype == thrift::TType::I32) encoding = decoder.ReadZigZag();
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};

struct PageHeader {
    PageType type = PageType::DATA_PAGE;
    int32_t uncompressed_page_size = 0;
    int32_t compressed_page_size = 0;
    DataPageHeader data_page_header;
    DictionaryPageHeader dictionary_page_header;
    
    void Parse(thrift::CompactDecoder& decoder) {
        thrift::TType ftype;
        int16_t fid;
        int16_t old_id = decoder.last_field_id_;
        decoder.last_field_id_ = 0;
        
        while (decoder.ReadFieldBegin(ftype, fid)) {
            if (fid == 1 && ftype == thrift::TType::I32) type = static_cast<PageType>(decoder.ReadZigZag());
            else if (fid == 2 && ftype == thrift::TType::I32) uncompressed_page_size = decoder.ReadZigZag();
            else if (fid == 3 && ftype == thrift::TType::I32) compressed_page_size = decoder.ReadZigZag();
            else if (fid == 5 && ftype == thrift::TType::STRUCT) data_page_header.Parse(decoder);
            else if (fid == 7 && ftype == thrift::TType::STRUCT) dictionary_page_header.Parse(decoder);
            else decoder.Skip(ftype);
        }
        decoder.last_field_id_ = old_id;
    }
};


// --- src/delta_decoder.h ---

namespace decoders {

class DeltaBinaryPackedDecoder {
public:
    DeltaBinaryPackedDecoder(const uint8_t* data, size_t size) 
        : ptr_(data), end_(data + size) {
        if (size > 0) ReadHeader();
    }

    uint64_t ReadUleb128() {
        uint64_t result = 0;
        int shift = 0;
        while (ptr_ < end_) {
            uint8_t byte = *ptr_++;
            result |= (static_cast<uint64_t>(byte & 0x7F) << shift);
            if ((byte & 0x80) == 0) return result;
            shift += 7;
        }
        throw ParquetException("Invalid ULEB128 in DeltaBinaryPacked");
    }

    int64_t ReadZigZag() {
        uint64_t uleb = ReadUleb128();
        return (uleb >> 1) ^ -(uleb & 1);
    }

    void ReadHeader() {
        block_size_ = ReadUleb128();
        miniblocks_in_block_ = ReadUleb128();
        total_value_count_ = ReadUleb128();
        last_value_ = ReadZigZag();
        
        values_read_ = 0;
        values_left_in_block_ = 0;
        values_left_in_miniblock_ = 0;
        if (miniblocks_in_block_ > 0) {
            miniblock_size_ = block_size_ / miniblocks_in_block_;
        } else {
            miniblock_size_ = 0;
        }
    }

    void ReadBlockHeader() {
        min_delta_ = ReadZigZag();
        bit_widths_.clear();
        for (uint32_t i = 0; i < miniblocks_in_block_; ++i) {
            if (ptr_ >= end_) throw ParquetException("End of DeltaBinaryPacked stream");
            bit_widths_.push_back(*ptr_++);
        }
        miniblock_idx_ = 0;
        values_left_in_block_ = block_size_;
    }

    uint64_t ReadBits(int bit_width) {
        if (bit_width == 0) return 0;
        uint64_t result = 0;
        int bits_read = 0;
        
        while (bits_read < bit_width) {
            if (ptr_ >= end_) throw ParquetException("End of DeltaBinaryPacked bit stream");
            int bits_to_read = std::min(bit_width - bits_read, 8 - bit_offset_);
            uint8_t byte = (*ptr_ >> bit_offset_) & ((1 << bits_to_read) - 1);
            result |= (static_cast<uint64_t>(byte) << bits_read);
            
            bits_read += bits_to_read;
            bit_offset_ += bits_to_read;
            
            if (bit_offset_ == 8) {
                bit_offset_ = 0;
                ptr_++;
            }
        }
        return result;
    }

    template<typename T>
    bool Next(T& out) {
        if (values_read_ >= total_value_count_) return false;

        if (values_read_ == 0) {
            out = last_value_;
            values_read_++;
            return true;
        }

        if (values_left_in_block_ == 0) {
            ReadBlockHeader();
        }

        if (values_left_in_miniblock_ == 0) {
            if (miniblock_idx_ >= bit_widths_.size()) throw ParquetException("Invalid miniblock index");
            current_bit_width_ = bit_widths_[miniblock_idx_++];
            values_left_in_miniblock_ = miniblock_size_;
        }

        uint64_t delta = ReadBits(current_bit_width_);
        int64_t actual_delta = static_cast<int64_t>(delta) + min_delta_;
        last_value_ += actual_delta;
        
        out = static_cast<T>(last_value_);
        
        values_left_in_miniblock_--;
        values_left_in_block_--;
        values_read_++;
        return true;
    }

    const uint8_t* GetPtr() const { return ptr_; }

private:
    const uint8_t* ptr_;
    const uint8_t* end_;
    
    uint32_t block_size_ = 0;
    uint32_t miniblocks_in_block_ = 0;
    uint32_t total_value_count_ = 0;
    int64_t last_value_ = 0;
    
    uint32_t values_read_ = 0;
    uint32_t miniblock_size_ = 0;
    uint32_t values_left_in_block_ = 0;
    uint32_t values_left_in_miniblock_ = 0;
    uint32_t miniblock_idx_ = 0;
    
    int64_t min_delta_ = 0;
    std::vector<uint8_t> bit_widths_;
    int current_bit_width_ = 0;
    
    int bit_offset_ = 0;
};

class DeltaByteArrayDecoder {
public:
    DeltaByteArrayDecoder(const uint8_t* data, size_t size) 
        : ptr_(data), end_(data + size) {
        
        // The format is:
        // 1. DeltaBinaryPacked prefix lengths
        // 2. DeltaBinaryPacked suffix lengths
        // 3. Raw suffix bytes
        
        // But the DeltaBinaryPackedDecoder advances ptr_, so we decode all lengths first.
        DeltaBinaryPackedDecoder prefix_decoder(ptr_, end_ - ptr_);
        int32_t prefix_len;
        while (prefix_decoder.Next(prefix_len)) {
            prefix_lengths_.push_back(prefix_len);
        }
        
        ptr_ = prefix_decoder.GetPtr();
        
        DeltaBinaryPackedDecoder suffix_decoder(ptr_, end_ - ptr_);
        int32_t suffix_len;
        while (suffix_decoder.Next(suffix_len)) {
            suffix_lengths_.push_back(suffix_len);
        }
        
        ptr_ = suffix_decoder.GetPtr();
        values_count_ = std::min(prefix_lengths_.size(), suffix_lengths_.size());
    }

    bool Next(std::string& out) {
        if (current_idx_ >= values_count_) return false;
        
        int32_t prefix_len = prefix_lengths_[current_idx_];
        int32_t suffix_len = suffix_lengths_[current_idx_];
        
        if (ptr_ + suffix_len > end_) throw ParquetException("DeltaByteArray suffix reading out of bounds");
        
        std::string suffix(reinterpret_cast<const char*>(ptr_), suffix_len);
        ptr_ += suffix_len;
        
        std::string result;
        if (prefix_len > 0 && prefix_len <= last_val_.size()) {
            result = last_val_.substr(0, prefix_len) + suffix;
        } else {
            result = suffix;
        }
        
        last_val_ = result;
        out = result;
        current_idx_++;
        return true;
    }

private:
    const uint8_t* ptr_;
    const uint8_t* end_;
    std::vector<int32_t> prefix_lengths_;
    std::vector<int32_t> suffix_lengths_;
    size_t values_count_ = 0;
    size_t current_idx_ = 0;
    std::string last_val_ = "";
};

} // namespace decoders

// --- src/decompress.h ---

#ifdef TINYPARQUET_ENABLE_GZIP
#endif

#ifdef TINYPARQUET_ENABLE_ZSTD
#endif

#ifdef TINYPARQUET_ENABLE_BROTLI
#endif

namespace decompress {

inline bool SnappyUncompress(const uint8_t* in, size_t in_size, std::vector<uint8_t>& out) {
    const uint8_t* ptr = in;
    const uint8_t* end = in + in_size;

    uint32_t uncompressed_len = 0;
    int shift = 0;
    while (ptr < end) {
        uint8_t b = *ptr++;
        uncompressed_len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }

    out.clear();
    out.reserve(uncompressed_len);

    while (ptr < end) {
        uint8_t tag = *ptr++;
        int len, offset;

        if ((tag & 0x3) == 0x0) {
            len = (tag >> 2) + 1;
            if (len > 60) {
                int bytes = len - 60;
                len = 0;
                for (int i = 0; i < bytes; ++i) {
                    if (ptr >= end) return false;
                    len |= (*ptr++) << (8 * i);
                }
                len += 1;
            }
            if (ptr + len > end) return false;
            out.insert(out.end(), ptr, ptr + len);
            ptr += len;
        } else if ((tag & 0x3) == 0x1) {
            len = ((tag >> 2) & 0x7) + 4;
            if (ptr >= end) return false;
            offset = ((tag >> 5) << 8) | (*ptr++);
            size_t out_size = out.size();
            if (offset == 0 || offset > out_size) return false;
            for (int i = 0; i < len; ++i) out.push_back(out[out.size() - offset]);
        } else if ((tag & 0x3) == 0x2) {
            len = (tag >> 2) + 1;
            if (ptr + 2 > end) return false;
            offset = ptr[0] | (ptr[1] << 8);
            ptr += 2;
            size_t out_size = out.size();
            if (offset == 0 || offset > out_size) return false;
            for (int i = 0; i < len; ++i) out.push_back(out[out.size() - offset]);
        } else if ((tag & 0x3) == 0x3) {
            len = (tag >> 2) + 1;
            if (ptr + 4 > end) return false;
            offset = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
            ptr += 4;
            size_t out_size = out.size();
            if (offset == 0 || offset > out_size) return false;
            for (int i = 0; i < len; ++i) out.push_back(out[out.size() - offset]);
        }
    }
    return out.size() == uncompressed_len;
}

inline bool Lz4Uncompress(const uint8_t* in, size_t in_size, std::vector<uint8_t>& out, size_t uncompressed_len) {
    const uint8_t* ptr = in;
    const uint8_t* end = in + in_size;
    out.clear();
    out.reserve(uncompressed_len);
    
    while (ptr < end) {
        uint8_t token = *ptr++;
        int literals_len = token >> 4;
        if (literals_len == 15) {
            uint8_t l;
            do {
                if (ptr >= end) return false;
                l = *ptr++;
                literals_len += l;
            } while (l == 255);
        }
        
        if (ptr + literals_len > end) return false;
        out.insert(out.end(), ptr, ptr + literals_len);
        ptr += literals_len;
        
        if (ptr == end) break;
        
        if (ptr + 2 > end) return false;
        int offset = ptr[0] | (ptr[1] << 8);
        ptr += 2;
        if (offset == 0 || offset > out.size()) return false;
        
        int match_len = (token & 0x0f) + 4;
        if (match_len == 15 + 4) {
            uint8_t l;
            do {
                if (ptr >= end) return false;
                l = *ptr++;
                match_len += l;
            } while (l == 255);
        }
        
        for (int i = 0; i < match_len; ++i) {
            out.push_back(out[out.size() - offset]);
        }
    }
    return out.size() == uncompressed_len;
}

inline bool GzipUncompress(const uint8_t* in, size_t in_size, std::vector<uint8_t>& out, size_t uncompressed_len) {
#ifdef TINYPARQUET_ENABLE_GZIP
    out.resize(uncompressed_len);
    unsigned long dest_len = uncompressed_len;
    int res = mz_uncompress(out.data(), &dest_len, in, in_size);
    if (res != 0 /* MZ_OK */) return false;
    out.resize(dest_len);
    return true;
#else
    throw ParquetException("GZIP decompression requires defining TINYPARQUET_ENABLE_GZIP and linking miniz");
#endif
}

inline bool ZstdUncompress(const uint8_t* in, size_t in_size, std::vector<uint8_t>& out, size_t uncompressed_len) {
#ifdef TINYPARQUET_ENABLE_ZSTD
    out.resize(uncompressed_len);
    size_t const res = ZSTD_decompress(out.data(), uncompressed_len, in, in_size);
    if (ZSTD_isError(res)) return false;
    out.resize(res);
    return true;
#else
    throw ParquetException("ZSTD decompression requires defining TINYPARQUET_ENABLE_ZSTD and linking zstd");
#endif
}

inline bool BrotliUncompress(const uint8_t* in, size_t in_size, std::vector<uint8_t>& out, size_t uncompressed_len) {
#ifdef TINYPARQUET_ENABLE_BROTLI
    out.resize(uncompressed_len);
    size_t decoded_size = uncompressed_len;
    BrotliDecoderResult res = BrotliDecoderDecompress(in_size, in, &decoded_size, out.data());
    if (res != BROTLI_DECODER_RESULT_SUCCESS) return false;
    out.resize(decoded_size);
    return true;
#else
    throw ParquetException("BROTLI decompression requires defining TINYPARQUET_ENABLE_BROTLI and linking brotli");
#endif
}

} // namespace decompress

// --- src/decoders.h ---

namespace decoders {

class RleDecoder {
public:
    RleDecoder(const uint8_t* data, size_t size, int bit_width)
        : ptr_(data), end_(data + size), bit_width_(bit_width) {}

    uint32_t ReadHeader() {
        uint32_t result = 0;
        uint32_t shift = 0;
        while (ptr_ < end_) {
            uint8_t byte = *ptr_++;
            result |= static_cast<uint32_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) return result;
            shift += 7;
        }
        throw ParquetException("Invalid RLE header");
    }

    bool Next(uint32_t& value) {
        if (current_run_count_ == 0) {
            if (ptr_ >= end_) return false;
            uint32_t header = ReadHeader();
            is_rle_ = (header & 1) == 0;
            current_run_count_ = header >> 1;
            
            if (is_rle_) {
                // Read the value for the RLE run
                uint32_t val = 0;
                int bytes = (bit_width_ + 7) / 8;
                for (int i = 0; i < bytes; ++i) {
                    if (ptr_ < end_) val |= (*ptr_++) << (i * 8);
                }
                rle_value_ = val;
            } else {
                current_run_count_ *= 8; // bit-packed count is multiple of 8
            }
        }
        
        if (is_rle_) {
            value = rle_value_;
            current_run_count_--;
        } else {
            // Read bit-packed
            // Simplified: Just doing naive bit unpacking
            if (bit_offset_ == 0) {
                if (ptr_ >= end_) throw ParquetException("Unexpected end of bit-packed data");
                packed_byte_ = *ptr_++;
            }
            value = (packed_byte_ >> bit_offset_) & ((1 << bit_width_) - 1);
            bit_offset_ += bit_width_;
            if (bit_offset_ >= 8) {
                // Advanced bit-packing logic required for unaligned crosses,
                // but for bit_width=1 or 8 this works perfectly.
                bit_offset_ = 0; 
            }
            current_run_count_--;
        }
        return true;
    }

private:
    const uint8_t* ptr_;
    const uint8_t* end_;
    int bit_width_;
    uint32_t current_run_count_ = 0;
    bool is_rle_ = false;
    uint32_t rle_value_ = 0;
    uint8_t packed_byte_ = 0;
    int bit_offset_ = 0;
};

// PLAIN decoder for raw byte extraction
class PlainDecoder {
public:
    PlainDecoder(const uint8_t* data, size_t size) : ptr_(data), end_(data + size) {}

    bool ReadInt32(int32_t& out) {
        if (ptr_ + 4 > end_) return false;
        std::memcpy(&out, ptr_, 4);
        ptr_ += 4;
        return true;
    }

    bool ReadInt64(int64_t& out) {
        if (ptr_ + 8 > end_) return false;
        std::memcpy(&out, ptr_, 8);
        ptr_ += 8;
        return true;
    }

    bool ReadFloat(float& val) {
        if (ptr_ + 4 > end_) return false;
        std::memcpy(&val, ptr_, 4);
        ptr_ += 4;
        return true;
    }

    bool ReadByteArray(std::string& out) {
        if (ptr_ + 4 > end_) return false;
        uint32_t len = 0;
        std::memcpy(&len, ptr_, 4);
        ptr_ += 4;
        if (ptr_ + len > end_) return false;
        out.assign(reinterpret_cast<const char*>(ptr_), len);
        ptr_ += len;
        return true;
    }

    bool ReadDouble(double& val) {
        if (ptr_ + 8 > end_) return false;
        std::memcpy(&val, ptr_, 8);
        ptr_ += 8;
        return true;
    }



private:
    const uint8_t* ptr_;
    const uint8_t* end_;
};

} // namespace decoders

// --- src/page_reader.h ---


class PageReader {
public:
    PageReader(const uint8_t* ptr, const uint8_t* end) : ptr_(ptr), end_(end) {}

    bool NextPage(PageHeader& header, const uint8_t*& page_data) {
        if (ptr_ >= end_) return false;
        
        thrift::CompactDecoder decoder(ptr_, end_ - ptr_);
        header.Parse(decoder);
        
        size_t header_size = decoder.GetBytesRead();
        ptr_ += header_size;
        
        if (header.compressed_page_size < 0) {
            return false;
        }
        
        page_data = ptr_;
        ptr_ += header.compressed_page_size;
        
        if (ptr_ > end_) {
            return false;
        }
        
        return true;
    }

private:
    const uint8_t* ptr_;
    const uint8_t* end_;
};


// --- src/column_reader.h ---


class ColumnReader {
public:
    ColumnReader(const ColumnChunk& chunk, const uint8_t* file_data, size_t file_size, int max_def_level = 1) 
        : chunk_(chunk), data_(file_data), size_(file_size), max_def_level_(max_def_level) {
        
        uint64_t offset = chunk.meta_data.has_dictionary_page ? 
                          chunk.meta_data.dictionary_page_offset : 
                          chunk.meta_data.data_page_offset;
        
        if (offset == 0 && chunk.meta_data.has_dictionary_page) {
            offset = chunk.meta_data.data_page_offset;
        }
        if (offset == 0) {
            offset = chunk.file_offset;
        }
        
        page_reader_ = std::make_unique<PageReader>(data_ + offset, data_ + size_);
    }

    void ReadAllInt32(std::vector<int32_t>& out) {
        PageHeader header;
        const uint8_t* raw_page_data;
        int64_t total_values_to_read = chunk_.meta_data.num_values;
        int64_t values_read = 0;
        
        std::vector<int32_t> dictionary;
        std::vector<uint8_t> uncompressed_buffer;
        
        while (values_read < total_values_to_read && page_reader_->NextPage(header, raw_page_data)) {
            const uint8_t* page_data = raw_page_data;
            size_t page_size = header.uncompressed_page_size;
            
            if (chunk_.meta_data.codec == CompressionCodec::SNAPPY) {
                if (!decompress::SnappyUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer)) {
                    throw ParquetException("Failed to uncompress Snappy page");
                }
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::LZ4_RAW || chunk_.meta_data.codec == CompressionCodec::LZ4) {
                if (!decompress::Lz4Uncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) {
                    throw ParquetException("Failed to uncompress LZ4 page");
                }
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::GZIP) {
                if (!decompress::GzipUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress GZIP page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::ZSTD) {
                if (!decompress::ZstdUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress ZSTD page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::BROTLI) {
                if (!decompress::BrotliUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress BROTLI page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec != CompressionCodec::UNCOMPRESSED) {
                throw ParquetException("Unsupported compression codec");
            }

            if (header.type == PageType::DICTIONARY_PAGE) {
                decoders::PlainDecoder plain(page_data, page_size);
                for (int i = 0; i < header.dictionary_page_header.num_values; ++i) {
                    int32_t val;
                    if (plain.ReadInt32(val)) dictionary.push_back(val);
                }
            }
            else if (header.type == PageType::DATA_PAGE) {
                const uint8_t* ptr = page_data;
                uint32_t def_len = 0;
                std::vector<uint32_t> def_levels;
                
                if (page_size == 0) continue;

                if (max_def_level_ > 0) {
                    std::memcpy(&def_len, ptr, 4);
                    ptr += 4;
                    decoders::RleDecoder rle_def(ptr, def_len, 1);
                    uint32_t def_val;
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        if (rle_def.Next(def_val)) def_levels.push_back(def_val);
                        else break;
                    }
                    ptr += def_len;
                } else {
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        def_levels.push_back(1);
                    }
                }
                
                if (header.data_page_header.encoding == 2 || header.data_page_header.encoding == 8) {
                    int bit_width = *ptr++;
                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    if (max_def_level_ == 0) rle_data_len = page_size - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);
                    
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) { // not null
                            uint32_t index;
                            if (rle_data.Next(index)) {
                                if (index >= dictionary.size()) throw ParquetException("Dictionary index out of bounds");
                                out.push_back(dictionary[index]);
                            }
                        } else {
                            out.push_back(0); // null placeholder
                        }
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else if (header.data_page_header.encoding == 5) {
                    decoders::DeltaBinaryPackedDecoder delta_data(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            int32_t val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    // PLAIN encoded
                    size_t plain_len = page_size - 4 - def_len;
                    if (max_def_level_ == 0) plain_len = page_size;
                    decoders::PlainDecoder plain(ptr, plain_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) { // not null
                            int32_t val;
                            if (plain.ReadInt32(val)) {
                                out.push_back(val);
                            }
                        } else {
                            out.push_back(0); // placeholder for null
                        }
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                }
            }
        }
    }

private:
    const ColumnChunk& chunk_;
    const uint8_t* data_;
    size_t size_;
    int max_def_level_;
    std::unique_ptr<PageReader> page_reader_;
public:
    void ReadAllInt64(std::vector<int64_t>& out) {
        PageHeader header;
        const uint8_t* raw_page_data;
        int64_t total_values_to_read = chunk_.meta_data.num_values;
        int64_t values_read = 0;
        
        std::vector<int64_t> dictionary;
        std::vector<uint8_t> uncompressed_buffer;
        
        while (values_read < total_values_to_read && page_reader_->NextPage(header, raw_page_data)) {
            const uint8_t* page_data = raw_page_data;
            size_t page_size = header.uncompressed_page_size;
            
            if (chunk_.meta_data.codec == CompressionCodec::SNAPPY) {
                if (!decompress::SnappyUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer)) throw ParquetException("Failed to uncompress Snappy page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::LZ4_RAW || chunk_.meta_data.codec == CompressionCodec::LZ4) {
                if (!decompress::Lz4Uncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress LZ4 page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::GZIP) {
                if (!decompress::GzipUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress GZIP page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::ZSTD) {
                if (!decompress::ZstdUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress ZSTD page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::BROTLI) {
                if (!decompress::BrotliUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress BROTLI page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec != CompressionCodec::UNCOMPRESSED) {
                throw ParquetException("Unsupported compression codec");
            }
            
            if (header.type == PageType::DICTIONARY_PAGE) {
                decoders::PlainDecoder plain(page_data, page_size);
                for (int i = 0; i < header.dictionary_page_header.num_values; ++i) {
                    int64_t val;
                    if (plain.ReadInt64(val)) dictionary.push_back(val);
                }
            } else if (header.type == PageType::DATA_PAGE) {
                const uint8_t* ptr = page_data;
                uint32_t def_len = 0;
                std::vector<uint32_t> def_levels;
                
                if (page_size == 0) continue;

                if (max_def_level_ > 0) {
                    std::memcpy(&def_len, ptr, 4);
                    ptr += 4;
                    decoders::RleDecoder rle_def(ptr, def_len, 1);
                    uint32_t def_val;
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        if (rle_def.Next(def_val)) def_levels.push_back(def_val);
                        else break;
                    }
                    ptr += def_len;
                } else {
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        def_levels.push_back(1);
                    }
                }
                if (header.data_page_header.encoding == 2 || header.data_page_header.encoding == 8) {
                    int bit_width = *ptr++;
                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    if (max_def_level_ == 0) rle_data_len = page_size - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            uint32_t index;
                            if (rle_data.Next(index)) out.push_back(dictionary[index]);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            int64_t val;
                            if (plain.ReadInt64(val)) out.push_back(val);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                }
            }
        }
    }
    void ReadAllByteArray(std::vector<std::string>& out) {
        PageHeader header;
        const uint8_t* raw_page_data;
        int64_t total_values_to_read = chunk_.meta_data.num_values;
        int64_t values_read = 0;
        
        std::vector<std::string> dictionary;
        std::vector<uint8_t> uncompressed_buffer;
        
        while (values_read < total_values_to_read && page_reader_->NextPage(header, raw_page_data)) {
            const uint8_t* page_data = raw_page_data;
            size_t page_size = header.uncompressed_page_size;
            
            if (chunk_.meta_data.codec == CompressionCodec::SNAPPY) {
                if (!decompress::SnappyUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer)) throw ParquetException("Failed to uncompress Snappy page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::LZ4_RAW || chunk_.meta_data.codec == CompressionCodec::LZ4) {
                if (!decompress::Lz4Uncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress LZ4 page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::GZIP) {
                if (!decompress::GzipUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress GZIP page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::ZSTD) {
                if (!decompress::ZstdUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress ZSTD page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec == CompressionCodec::BROTLI) {
                if (!decompress::BrotliUncompress(raw_page_data, header.compressed_page_size, uncompressed_buffer, header.uncompressed_page_size)) throw ParquetException("Failed to uncompress BROTLI page");
                page_data = uncompressed_buffer.data();
            } else if (chunk_.meta_data.codec != CompressionCodec::UNCOMPRESSED) {
                throw ParquetException("Unsupported compression codec");
            }
            
            if (header.type == PageType::DICTIONARY_PAGE) {
                decoders::PlainDecoder plain(page_data, page_size);
                for (int i = 0; i < header.dictionary_page_header.num_values; ++i) {
                    std::string val;
                    if (plain.ReadByteArray(val)) dictionary.push_back(val);
                }
            } else if (header.type == PageType::DATA_PAGE) {
                const uint8_t* ptr = page_data;
                uint32_t def_len = 0;
                std::vector<uint32_t> def_levels;
                
                if (page_size == 0) continue;
                
                if (max_def_level_ > 0) {
                    std::memcpy(&def_len, ptr, 4);
                    ptr += 4;
                    decoders::RleDecoder rle_def(ptr, def_len, 1);
                    uint32_t def_val;
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        if (rle_def.Next(def_val)) def_levels.push_back(def_val);
                        else break;
                    }
                    ptr += def_len;
                } else {
                    for (int i = 0; i < header.data_page_header.num_values; ++i) {
                        def_levels.push_back(1);
                    }
                }
                
                if (header.data_page_header.encoding == 2 || header.data_page_header.encoding == 8) {
                    int bit_width = *ptr++;
                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    if (max_def_level_ == 0) rle_data_len = page_size - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            uint32_t index;
                            if (rle_data.Next(index)) out.push_back(dictionary[index]);
                        } else out.push_back("");
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else if (header.data_page_header.encoding == 7) {
                    decoders::DeltaByteArrayDecoder delta_data(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            std::string val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back("");
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    size_t plain_len = page_size - 4 - def_len;
                    if (max_def_level_ == 0) plain_len = page_size;
                    decoders::PlainDecoder plain(ptr, plain_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            std::string val;
                            if (plain.ReadByteArray(val)) out.push_back(val);
                        } else out.push_back("");
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                }
            }
        }
    }
};


// --- src/reader.h ---


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
        int max_def_level = 1;
        for (const auto& elem : metadata_.schema) {
            if (elem.name == name && elem.repetition_type == 0) {
                max_def_level = 0;
            }
        }
        for (const auto& chunk : metadata_.row_groups[0].columns) {
            if (!chunk.meta_data.path_in_schema.empty() && chunk.meta_data.path_in_schema[0] == name) {
                return ColumnReader(chunk, mmap_.data(), mmap_.size(), max_def_level);
            }
        }
        throw ParquetException("Column not found");
    }

private:
    MemoryMappedFile mmap_;
    FileMetaData metadata_;
};


} // namespace tinyparquet
