#include <iostream>
#include "tinyparquet.hpp"

int main() {
    try {
        tinyparquet::Reader reader("alltypes_plain.parquet");
        auto metadata = reader.GetMetaData();
        std::cout << "Parquet file metadata parsed successfully!\n";
        std::cout << "Number of rows: " << metadata.num_rows << "\n";
        std::cout << "Number of Row Groups: " << metadata.row_groups.size() << "\n";
        std::cout << "Schema elements: " << metadata.schema.size() << "\n";
        for (const auto& elem : metadata.schema) {
            std::cout << " - Column: " << elem.name << " (rep: " << elem.repetition_type << ")\n";
        }
        
        std::cout << "\nExtracting int_col (index 5)...\n";
        
        std::vector<int32_t> int_values;
        auto int_reader = reader.GetColumnReader("int_col");
        int_reader.ReadAllInt32(int_values);
        
        std::cout << "Values in int_col: ";
        for (auto v : int_values) {
            std::cout << v << " ";
        }
        std::cout << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
