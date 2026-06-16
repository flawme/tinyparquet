import re

content = open('src/column_reader.h', 'r').read()

# Add DELTA_BINARY_PACKED to ReadAllInt32
content = content.replace('''                } else {
                    // PLAIN encoded''', '''                } else if (header.data_page_header.encoding == 5) {
                    // DELTA_BINARY_PACKED encoded
                    decoders::DeltaBinaryPackedDecoder delta_data(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            int32_t val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    // PLAIN encoded''')

# Add DELTA_BINARY_PACKED to ReadAllInt64
content = content.replace('''                } else {
                    size_t plain_len = page_size - 4 - def_len;''', '''                } else if (header.data_page_header.encoding == 5) {
                    decoders::DeltaBinaryPackedDecoder delta_data(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            int64_t val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back(0);
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    size_t plain_len = page_size - 4 - def_len;''')

# Add DELTA_BYTE_ARRAY to ReadAllByteArray
content = content.replace('''                } else {
                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);''', '''                } else if (header.data_page_header.encoding == 7) {
                    decoders::DeltaByteArrayDecoder delta_data(ptr, page_size - 4 - def_len);
                    for (size_t i = 0; i < def_levels.size(); ++i) {
                        if (def_levels[i] == 1) {
                            std::string val;
                            if (delta_data.Next(val)) out.push_back(val);
                        } else out.push_back("");
                        values_read++;
                        if (values_read >= total_values_to_read) break;
                    }
                } else {
                    decoders::PlainDecoder plain(ptr, page_size - 4 - def_len);''')

open('src/column_reader.h', 'w').write(content)
