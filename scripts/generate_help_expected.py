#!/usr/bin/env python3
"""Generate expected output file for help regression test.

This generates the expected psql output format for the HELP command test.
The format matches what psql produces when displaying a single-row table
with COMMAND and HELP TEXT columns.
"""

import sys
import os

# Change to project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
os.chdir(project_root)

# Read the generated help text
with open('yacc/console/help/CREATE_KEY_RANGE.txt', 'r') as f:
    help_text = f.read()

# Split into lines, keeping trailing empty line if present
lines = help_text.split('\n')
# Remove trailing empty strings but keep content structure
while lines and lines[-1] == '':
    lines.pop()

# Find max line length - psql uses this to size the column
max_len = max(len(line) for line in lines)

# psql format: command column width is based on header "COMMAND" (7 chars) 
# padded to accommodate "CREATE KEY RANGE" (16 chars) + 2 spaces
# Total: 18 chars for first column
cmd = "CREATE KEY RANGE"
cmd_col_width = 18

# Help text column width = max content length + 1 for leading space
help_col_width = max_len + 1

output_lines = []
output_lines.append("HELP CREATE KEY RANGE;")

# Header line - psql centers column names and pads with spaces
# Format: " COMMAND      |      HELP TEXT      " (varies based on content width)
cmd_header = "COMMAND".center(cmd_col_width)
help_header = "HELP TEXT".center(help_col_width)
header = f"{cmd_header}|{help_header}"
output_lines.append(header)

# Separator line - dashes matching column widths
separator = "-" * cmd_col_width + "+" + "-" * help_col_width
output_lines.append(separator)

# Data rows
for i, line in enumerate(lines):
    if i == 0:
        # First row shows command name
        row = f" {cmd:<{cmd_col_width-1}}| {line:<{max_len}}"
    else:
        # Continuation rows have empty command column
        row = f" {'':<{cmd_col_width-1}}| {line:<{max_len}}"
    
    # Add continuation marker (+) if not last line
    if i < len(lines) - 1:
        row += "+"
    output_lines.append(row)

# Row count
output_lines.append("(1 row)")
output_lines.append("")

result = '\n'.join(output_lines)
with open('test/regress/tests/console/expected/help.out', 'w') as f:
    f.write(result)

print("Generated expected file successfully!")
print(f"Max content line length: {max_len}")
print(f"Help column width: {help_col_width}")
