#pragma once
#include "common.h"
#include "metadata.h"
#include "page_reader.h"
#include "decoders.h"
#include "decompress.h"

namespace tinyparquet {

class ColumnReader {
public:
    ColumnReader(const ColumnChunk& chunk, const uint8_t* file_data, size_t file_size, int max_def_level = 1) 
        : chunk_(chunk), data_(file_data), size_(file_size), max_def_level_(max_def_level) {
        
        uint64_t offset = chunk.meta_data.has_dictionary_page ? 
                          chunk.meta_data.dictionary_page_offset : 
                          chunk.meta_data.data_page_offset;
        
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
                std::memcpy(&def_len, ptr, 4);
                ptr += 4;
                
                decoders::RleDecoder rle_def(ptr, def_len, 1);
                std::vector<uint32_t> def_levels;
                uint32_t def_val;
                for (int i = 0; i < header.data_page_header.num_values; ++i) {
                    if (rle_def.Next(def_val)) def_levels.push_back(def_val);
                    else break;
                }
                
                ptr += def_len;
                
                if (header.data_page_header.encoding == 2 || header.data_page_header.encoding == 8) {
                    int bit_width = *ptr++;
                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);
                    
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) { // not null
                            uint32_t index;
                            if (rle_data.Next(index)) {
                                out.push_back(dictionary[index]);
                            }
                        } else {
                            out.push_back(0); // null placeholder
                        }
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    // PLAIN encoded
                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);
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
                std::memcpy(&def_len, ptr, 4);
                ptr += 4;
                
                decoders::RleDecoder rle_def(ptr, def_len, 1);
                std::vector<uint32_t> def_levels;
                uint32_t def_val;
                for (int i = 0; i < header.data_page_header.num_values; ++i) {
                    if (rle_def.Next(def_val)) def_levels.push_back(def_val);
                    else break;
                }
                
                ptr += def_len;
                if (header.data_page_header.encoding == 2 || header.data_page_header.encoding == 8) {
                    int bit_width = *ptr++;
                    size_t rle_data_len = page_size - 4 - def_len - 1;
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

} // namespace tinyparquet
