Data Quality Analyzer
This Python script performs data quality checks on large datasets, such as CSV and Parquet files, to ensure data integrity and consistency. It focuses on identifying issues such as null values, duplicate entries, empty strings, and unique records for each column. The results, including the time taken to process each column, are stored in an output CSV file.

Key Features:
File Format Support: The script supports both CSV and Parquet file formats. It automatically detects the file type based on the input file extension.

Data Quality Metrics:

Null Values: Counts how many entries in a column are null.
Duplicate Values: Counts how many records are duplicates (only for non-null string data).
Empty String Count: Detects and counts empty or whitespace-only strings in text fields.
Unique Values: Tracks how many unique values exist per column.
Data Type: Captures the data type of each column (e.g., integer, float, object).
Total Records: Provides the total record count for each column, ensuring there is no discrepancy.
Performance Tracking: The script tracks and logs the time taken to analyze each column, ensuring efficiency for large datasets.

Error Handling: Handles various file reading issues, such as encoding problems and missing data. Non-UTF-8 encodings are handled via fallback mechanisms to ensure robustness.

Logging: Uses the loguru library for detailed logging, including file read times and data quality analysis durations. The log files are automatically rotated based on size.

Custom Output: Results are written to a clean CSV output file, summarizing the data quality for each column.

Usage:
To run the script, pass the input file path and output file path as command-line arguments:

php
Copy code
python3 analysing_file.py <input_file> <output_file>
For example:

kotlin
Copy code
python3 analysing_file.py data.csv report.csv
System Requirements:
Python 3.x
Required libraries: pandas, pathlib, loguru, and pyarrow (for Parquet support)
Error Handling:
If the script encounters encoding issues (e.g., UnicodeDecodeError), it uses fallback encodings to continue processing. Invalid data or corrupted rows are flagged in the output report.

