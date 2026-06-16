import os
import re
from datetime import datetime

# Files to combine in order
HEADERS = [
    "src/common.h",
    "src/mmap.h",
    "src/thrift.h",
    "src/metadata.h",
    "src/delta_decoder.h",
    "src/decompress.h",
    "src/decoders.h",
    "src/page_reader.h",
    "src/column_reader.h",
    "src/reader.h"
]

def combine():
    out_file = "tinyparquet.hpp"
    with open(out_file, "w") as out:
        out.write(f"// TinyParquet - Single Header Parquet Reader\n")
        out.write(f"// Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        out.write("#pragma once\n\n")
        
        # Include standard headers once
        std_includes = set()
        
        # First pass to collect std includes
        for f in HEADERS:
            if not os.path.exists(f): continue
            with open(f, "r") as src:
                for line in src:
                    if line.startswith("#include <"):
                        std_includes.add(line.strip())
        
        for inc in sorted(list(std_includes)):
            out.write(inc + "\n")
        out.write("\nnamespace tinyparquet {\n\n")
        
        # Second pass to write content
        for f in HEADERS:
            if not os.path.exists(f): continue
            out.write(f"// --- {f} ---\n")
            with open(f, "r") as src:
                for line in src:
                    # skip pragma once, internal includes, and std includes
                    if line.startswith("#pragma once"): continue
                    if line.startswith("#include \""): continue
                    if line.startswith("#include <"): continue
                    if line.startswith("namespace tinyparquet {"): continue
                    if line.startswith("} // namespace tinyparquet"): continue
                    out.write(line)
            out.write("\n")
            
        out.write("} // namespace tinyparquet\n")
    print(f"Amalgamated into {out_file} successfully.")

if __name__ == "__main__":
    combine()
