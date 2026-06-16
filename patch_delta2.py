import re

content = open('src/column_reader.h', 'r').read()

def inject(func_name, to_replace, replacement):
    idx = content.find(func_name)
    if idx == -1:
        print(f"Function {func_name} not found")
        return
    # find the next instance of to_replace
    rep_idx = content.find(to_replace, idx)
    if rep_idx == -1:
        print(f"to_replace not found in {func_name}")
        return
    # replace exactly one instance
    return content[:rep_idx] + content[rep_idx:].replace(to_replace, replacement, 1)

content = inject("ReadAllInt32", 
'''                } else {
                    // PLAIN encoded''', 
'''                } else if (header.data_page_header.encoding == 5) {
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

content = inject("ReadAllInt64", 
'''                } else {
                    size_t plain_len = page_size - 4 - def_len;''', 
'''                } else if (header.data_page_header.encoding == 5) {
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

content = inject("ReadAllByteArray", 
'''                } else {
                    size_t plain_len = page_size - 4 - def_len;''', 
'''                } else if (header.data_page_header.encoding == 7) {
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
                    size_t plain_len = page_size - 4 - def_len;''')

open('src/column_reader.h', 'w').write(content)
