# Count lines in a text file

filename = "rtk_log.txt"

with open(filename, 'r') as f:
    line_count = sum(1 for _ in f)

print(f"Total lines: {line_count}")