#pragma once
#include "common.h"
#include <vector>

namespace tinyparquet {
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

} // namespace decompress
} // namespace tinyparquet
