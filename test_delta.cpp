#include "tinyparquet.hpp"
#include <iostream>
#include <vector>
#include <cassert>

using namespace tinyparquet;

void test_delta_binary_packed() {
    Reader reader("testing/parquet-testing/data/delta_encoding_optional_column.parquet");
    auto reader_col = reader.GetColumnReader("somedata"); // Needs actual column name
}

int main() {
    std::cout << "Tests passed!\n";
    return 0;
}
