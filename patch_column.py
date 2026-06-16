import re

with open('src/column_reader.h', 'r') as f:
    content = f.read()

# Fix ReadAllInt32 DeltaBinaryPacked buffer and num_values
content = content.replace('decoders::DeltaBinaryPackedDecoder delta_data(ptr, page_size - 4 - def_len);',
                          'size_t data_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0);\n                    decoders::DeltaBinaryPackedDecoder delta_data(ptr, data_len, header.data_page_header.num_values);')

# Fix ReadAllByteArray DeltaByteArray buffer and num_values
content = content.replace('decoders::DeltaByteArrayDecoder delta_data(ptr, page_size - 4 - def_len);',
                          'size_t data_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0);\n                    decoders::DeltaByteArrayDecoder delta_data(ptr, data_len, header.data_page_header.num_values);')

# Fix ReadAllInt64 missing encoding == 5 and fix buffer len
int64_delta = """                } else if (header.data_page_header.encoding == 5) {
                    size_t data_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0);
                    decoders::DeltaBinaryPackedDecoder delta_data(ptr, data_len, header.data_page_header.num_values);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            int64_t val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {"""

content = content.replace('                } else {\n                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);\n                    for (size_t i = 0; i < def_levels.size(); ++i) {', 
                          int64_delta + '\n                    size_t plain_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0);\n                    decoders::PlainDecoder plain(ptr, plain_len);\n                    for (size_t i = 0; i < def_levels.size(); ++i) {')

# Now fix RLE length logic for all three:
content = content.replace('size_t rle_data_len = page_size - 4 - def_len - 1;\n                    if (max_def_level_ == 0) rle_data_len = page_size - 1;',
                          'size_t rle_data_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0) - 1;')

# Now fix PLAIN length logic for Int32 and ByteArray
content = content.replace('size_t plain_len = page_size - 4 - def_len;\n                    if (max_def_level_ == 0) plain_len = page_size;',
                          'size_t plain_len = page_size - (max_def_level_ > 0 ? 4 + def_len : 0);')


with open('src/column_reader.h', 'w') as f:
    f.write(content)
