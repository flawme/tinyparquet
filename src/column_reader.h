#pragma once
#include "common.h"
#include "metadata.h"
#include "page_reader.h"
#include "decoders.h"

namespace tinyparquet {

class ColumnReader {
public:
    ColumnReader(const ColumnChunk& chunk, const uint8_t* file_data, size_t file_size) 
        : chunk_(chunk), data_(file_data), size_(file_size) {
        
        uint64_t offset = chunk.meta_data.has_dictionary_page ? 
                          chunk.meta_data.dictionary_page_offset : 
                          chunk.meta_data.data_page_offset;
        
        page_reader_ = std::make_unique<PageReader>(data_ + offset, data_ + size_);
    }

    void ReadAllInt32(std::vector<int32_t>& out) {
        PageHeader header;
        const uint8_t* page_data;
        int64_t total_values_to_read = chunk_.meta_data.num_values;
        int64_t values_read = 0;
        
        std::vector<int32_t> dictionary;
        
        while (values_read < total_values_to_read && page_reader_->NextPage(header, page_data)) {
            if (header.type == PageType::DICTIONARY_PAGE) {
                decoders::PlainDecoder plain(page_data, header.compressed_page_size);
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
                    // Dictionary encoded data values
                    int bit_width = *ptr++;
                    // The rest of the page is RLE encoded indices.
                    // Wait, RLE data length is NOT prefixed in V1 data pages!
                    // It just takes up the rest of the page.
                    size_t rle_data_len = header.compressed_page_size - 4 - def_len - 1;
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
                    decoders::PlainDecoder plain(ptr, header.compressed_page_size - 4 - def_len);
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
    ColumnChunk chunk_;
    const uint8_t* data_;
    size_t size_;
    std::unique_ptr<PageReader> page_reader_;
};

} // namespace tinyparquet
