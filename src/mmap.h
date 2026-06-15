#pragma once
#include "common.h"
#include <string>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tinyparquet {

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

} // namespace tinyparquet
