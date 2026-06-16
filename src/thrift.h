#pragma once
#include "common.h"
#include <vector>
#include <string>

namespace tinyparquet {
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
} // namespace tinyparquet
