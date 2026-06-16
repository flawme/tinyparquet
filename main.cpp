#include "tinyparquet.hpp"
#include <iostream>
#include <vector>

int main() {
    try {
        tinyparquet::Reader reader("lz4_raw_compressed_larger.parquet");
        auto metadata = reader.GetMetaData();
        std::cout << "Parquet file metadata parsed successfully!\n";
        std::cout << "Number of rows: " << metadata.num_rows << "\n";
        
        std::cout << "Schema elements: " << metadata.schema.size() << "\n";
        for (const auto& elem : metadata.schema) {
            std::cout << " - Column: " << elem.name << " (rep: " << elem.repetition_type << ", type: " << (int)elem.type << ")\n";
        }
        
        // Find the first physical column
        if (metadata.schema.size() > 1) {
            std::string col_name = metadata.schema[1].name;
            std::cout << "\nExtracting " << col_name << "...\n";
            auto col_reader = reader.GetColumnReader(col_name);
            
            if (metadata.schema[1].type == tinyparquet::Type::INT32) {
                std::vector<int32_t> values;
                col_reader.ReadAllInt32(values);
                std::cout << "Values in " << col_name << ": ";
                for (auto v : values) std::cout << v << " ";
                std::cout << "\n";
            } else if (metadata.schema[1].type == tinyparquet::Type::INT64) {
                std::vector<int64_t> values;
                col_reader.ReadAllInt64(values);
                std::cout << "Values in " << col_name << ": ";
                for (auto v : values) std::cout << v << " ";
                std::cout << "\n";
            } else if (metadata.schema[1].type == tinyparquet::Type::BYTE_ARRAY) {
                std::vector<std::string> values;
                col_reader.ReadAllByteArray(values);
                std::cout << "Values in " << col_name << ": ";
                for (auto v : values) std::cout << "[" << v << "] ";
                std::cout << "\n";
            } else {
                std::cout << "Unsupported type for printing.\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
