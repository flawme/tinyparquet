#pragma once
#include "common.h"
#include <vector>
#include <cstring>

namespace tinyparquet {
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
        if (len > static_cast<size_t>(end_ - ptr_)) return false;
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
} // namespace tinyparquet
