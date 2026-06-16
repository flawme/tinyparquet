#pragma once
#include "common.h"
#include <vector>
#include <cstring>
#include <string>
#include <iostream>

namespace tinyparquet {
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
        
        if (suffix_len < 0 || suffix_len > static_cast<int64_t>(end_ - ptr_)) throw ParquetException("DeltaByteArray suffix reading out of bounds");
        
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
} // namespace tinyparquet
