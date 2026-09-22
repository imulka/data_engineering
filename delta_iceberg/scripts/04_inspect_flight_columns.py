import csv

INPUT_FILE = (
    "data/raw/"
    "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2026_7.csv"
)

with open(INPUT_FILE, newline="", encoding="utf-8-sig") as file:
    reader = csv.reader(file)
    header = next(reader)

print(f"\nTotal columns found: {len(header)}\n")

for number, column in enumerate(header, start=1):
    column_name = column if column else "<BLANK COLUMN>"
    print(f"{number:3}: {column_name}")