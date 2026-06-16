#pragma once
#include "common.h"
#include "thrift.h"

namespace tinyparquet {

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

} // namespace tinyparquet
