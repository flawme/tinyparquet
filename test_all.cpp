#include "tinyparquet.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <filesystem>

void test_file(const std::string& filename) {
    std::cout << "Testing file: " << filename << std::endl;
    try {
        tinyparquet::Reader reader(filename);
        auto metadata = reader.GetMetaData();
        
        for (size_t i = 1; i < metadata.schema.size(); ++i) {
            const auto& elem = metadata.schema[i];
            std::string col_name = elem.name;
            try {
                auto col_reader = reader.GetColumnReader(col_name);
                if (elem.type == tinyparquet::Type::INT32) {
                    std::vector<int32_t> values;
                    col_reader.ReadAllInt32(values);
                } else if (elem.type == tinyparquet::Type::INT64) {
                    std::vector<int64_t> values;
                    col_reader.ReadAllInt64(values);
                } else if (elem.type == tinyparquet::Type::BYTE_ARRAY || elem.type == tinyparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    std::vector<std::string> values;
                    col_reader.ReadAllByteArray(values);
                }
            } catch (const std::exception& e) {
                // Not failing the process, just catching to allow next
                std::cerr << "    [ERROR] extracting column " << col_name << ": " << e.what() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] reading file " << filename << ": " << e.what() << "\n";
    }
}

int main() {
    std::string path = "testing/parquet-testing/data";
    for (const auto & entry : std::filesystem::recursive_directory_iterator(path)) {
        if (entry.path().extension() == ".parquet") {
            test_file(entry.path().string());
        }
    }
    return 0;
}
