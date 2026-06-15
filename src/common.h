#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>
#include <memory>

namespace tinyparquet {

// Exception class for Parquet parsing errors
class ParquetException : public std::runtime_error {
public:
    explicit ParquetException(const std::string& msg) : std::runtime_error(msg) {}
};

} // namespace tinyparquet
