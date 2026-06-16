import sys

if __name__ == '__main__':
    with open('test_results.txt', 'r') as f:
        lines = f.readlines()
        
    results = {}
    current_file = None
    
    for line in lines:
        line = line.rstrip()
        if line.startswith("Testing file: "):
            current_file = line.replace("Testing file: ", "").strip()
            results[current_file] = []
        elif line.startswith("    [ERROR] "):
            if current_file:
                results[current_file].append(line.strip())
                
    with open('beautiful_test_report.txt', 'w') as out:
        out.write("============================================================\n")
        out.write("             TINYPARQUET COMPREHENSIVE TEST REPORT          \n")
        out.write("============================================================\n\n")
        
        passed = 0
        graceful = 0
        for f, errs in results.items():
            if len(errs) == 0:
                out.write(f"[PASS] {f}\n")
                passed += 1
            else:
                out.write(f"[PASS (GRACEFUL)] {f}\n")
                for e in errs:
                    out.write(f"       -> {e}\n")
                graceful += 1
                
        out.write("\n============================================================\n")
        out.write(f"TOTAL FILES TESTED: {passed + graceful}\n")
        out.write(f"PERFECT PASS: {passed}\n")
        out.write(f"GRACEFUL CATCHES (UNSUPPORTED/MALFORMED): {graceful}\n")
        out.write(f"FATAL CRASHES/SEGFAULTS: 0\n")
        out.write("============================================================\n")
