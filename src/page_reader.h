#pragma once
#include "common.h"
#include "metadata.h"
#include "thrift.h"

namespace tinyparquet {

class PageReader {
public:
    PageReader(const uint8_t* ptr, const uint8_t* end) : ptr_(ptr), end_(end) {}

    bool NextPage(PageHeader& header, const uint8_t*& page_data) {
        if (ptr_ >= end_) return false;
        
        thrift::CompactDecoder decoder(ptr_, end_ - ptr_);
        header.Parse(decoder);
        
        size_t header_size = decoder.GetBytesRead();
        ptr_ += header_size;
        
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

} // namespace tinyparquet
