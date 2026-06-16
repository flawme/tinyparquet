import re

content = open('src/column_reader.h', 'r').read()

content = content.replace('''                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);''', '''                    size_t rle_data_len = page_size - 4 - def_len - 1;
                    if (max_def_level_ == 0) rle_data_len = page_size - 1;
                    decoders::RleDecoder rle_data(ptr, rle_data_len, bit_width);''')

content = content.replace('''                            if (rle_data.Next(index)) {
                                out.push_back(dictionary[index]);
                            }''', '''                            if (rle_data.Next(index)) {
                                if (index >= dictionary.size()) throw ParquetException("Dictionary index out of bounds");
                                out.push_back(dictionary[index]);
                            }''')

content = content.replace('''                    // PLAIN encoded
                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);''', '''                    // PLAIN encoded
                    size_t plain_len = page_size - 4 - def_len;
                    if (max_def_level_ == 0) plain_len = page_size;
                    decoders::PlainDecoder plain(ptr, plain_len);''')

open('src/column_reader.h', 'w').write(content)
