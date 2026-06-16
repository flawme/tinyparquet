#include "tinyparquet.hpp"
#include <iostream>
#include <vector>
#include <string>

void test_file(const std::string& filename) {
    std::cout << "Testing file: " << filename << std::endl;
    try {
        tinyparquet::Reader reader(filename);
        auto metadata = reader.GetMetaData();
        std::cout << "  Metadata parsed. Rows: " << metadata.num_rows << ", Schema size: " << metadata.schema.size() << "\n";
        
        for (size_t i = 1; i < metadata.schema.size(); ++i) {
            const auto& elem = metadata.schema[i];
            std::string col_name = elem.name;
            std::cout << "  - Extracting column: " << col_name << " (type: " << (int)elem.type << ")" << std::endl;
            try {
                auto col_reader = reader.GetColumnReader(col_name);
                if (elem.type == tinyparquet::Type::INT32) {
                    std::vector<int32_t> values;
                    std::cout << "    [DEBUG] Calling ReadAllInt32..." << std::endl;
                    col_reader.ReadAllInt32(values);
                    std::cout << "    Read " << values.size() << " INT32 values." << std::endl;
                } else if (elem.type == tinyparquet::Type::INT64) {
                    std::vector<int64_t> values;
                    col_reader.ReadAllInt64(values);
                    std::cout << "    Read " << values.size() << " INT64 values.\n";
                } else if (elem.type == tinyparquet::Type::BYTE_ARRAY || elem.type == tinyparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    std::vector<std::string> values;
                    col_reader.ReadAllByteArray(values);
                    std::cout << "    Read " << values.size() << " BYTE_ARRAY values.\n";
                } else {
                    std::cout << "    Unsupported type for extraction.\n";
                }
            } catch (const std::exception& e) {
                std::cerr << "    [ERROR] extracting column " << col_name << ": " << e.what() << "\n";
            }
        }
        std::cout << "  Success on file: " << filename << "\n";
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] reading file " << filename << ": " << e.what() << "\n";
    }
}

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        test_file(argv[i]);
        std::cout << "----------------------------------------\n";
    }
    return 0;
}
