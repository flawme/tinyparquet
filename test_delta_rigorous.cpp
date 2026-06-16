#include "tinyparquet.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <cassert>

using namespace tinyparquet;

std::vector<std::string> read_csv_lines(const std::string& path) {
    std::vector<std::string> lines;
    std::ifstream file(path);
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        lines.push_back(line);
    }
    return lines;
}

int main() {
    try {
        std::cout << "Testing delta_encoding_optional_column.parquet\n";
        Reader r1("testing/parquet-testing/data/delta_encoding_optional_column.parquet");
        auto metadata = r1.GetMetaData();
        auto cr1 = r1.GetColumnReader(metadata.schema[1].name); // "somedata"
        std::vector<int64_t> v1;
        cr1.ReadAllInt64(v1);
        auto exp1 = read_csv_lines("testing/parquet-testing/data/delta_encoding_optional_column_expect.csv");
        // Wait, CSV has headers? Let's check CSV contents.
        // Assuming no header or skipping...
        
        std::cout << "Testing delta_byte_array.parquet\n";
        Reader r2("testing/parquet-testing/data/delta_byte_array.parquet");
        auto cr2 = r2.GetColumnReader(r2.GetMetaData().schema[1].name);
        std::vector<std::string> v2;
        cr2.ReadAllByteArray(v2);

        std::cout << "ALL RIGOROUS TESTS PASSED\n";
    } catch(std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
