import pandas as pd
import time
import sys
from pathlib import Path
from loguru import logger
import chardet

# Configure the logger
logger.add("file_{time}.log", rotation="1 MB", level="INFO", format="{time} {level} {message}")

# Detect the encoding of the file to avoid UnicodeDecodeError
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  # Detect encoding from the first 10,000 bytes
    return result['encoding']

# Function to read a file (either CSV or Parquet)
def read_file(file_path):
    start_time = time.time()
    
    if file_path.suffix == '.csv':
        encoding = detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding)
    elif file_path.suffix == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Parquet file.")
    
    end_time = time.time()
    logger.info(f"File reading took {end_time - start_time:.2f} seconds.")
    
    return df

# Function to perform data quality checks for each column
def check_data_quality(df):
    data_quality = []
    total_records = len(df)  # Total number of records in the DataFrame

    for column in df.columns:
        start_time = time.time()

        col_data = df[column]
        column_total_records = len(col_data)  # Total number of records for the current column
        
        # Null values
        null_count = col_data.isnull().sum()

        # Empty values (either empty strings or just whitespace)
        empty_count = col_data.apply(lambda x: isinstance(x, str) and (x.strip() == '')).sum()

        # Null string values (strings like 'null' considered as null)
        null_string_count = col_data.apply(lambda x: isinstance(x, str) and x.lower() == 'null').sum()

        # Duplicate values (excluding null values)
        duplicate_count = col_data[col_data.notnull()].duplicated().sum()

        # Unique value count
        unique_count = col_data.nunique()

        # Time taken to process the column
        time_taken = time.time() - start_time

        # Skip columns where there is no relevant data (i.e., counts for null, duplicate, empty, or unique are all zero)
        if null_count == 0 and empty_count == 0 and null_string_count == 0 and duplicate_count == 0 and unique_count == column_total_records:
            continue  # Skip this column since it's irrelevant

        # Store only the numerical data (counts)
        data_quality.append({
            'Column': column,
            'Null Values': null_count,
            'Null String Values': null_string_count,
            'Duplicate Values': duplicate_count,
            'Empty Values': empty_count,
            'Unique Values': unique_count,
            'Total Records': column_total_records,  # This should match len(df)
            'Time Taken (seconds)': time_taken
        })

    return pd.DataFrame(data_quality)

# Function to save the data quality report to a CSV file (only numerical data)
def save_data_quality_to_csv(data_quality_df, output_file):
    data_quality_df.to_csv(output_file, index=False)
    logger.info(f"Data quality report saved to {output_file}")

# Main function to execute the steps
def analyze_file_and_save(input_file_path, output_file_path):
    start_time = time.time()

    file_path = Path(input_file_path)  # Convert to Path object for easier manipulation
    if not file_path.is_file():
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    df = read_file(file_path)
    data_quality_df = check_data_quality(df)
    save_data_quality_to_csv(data_quality_df, output_file_path)

    end_time = time.time()
    logger.info(f"Total analysis and saving took {end_time - start_time:.2f} seconds.")

# Example usage (replace with actual file paths)
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Please provide the input and output file paths.")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    analyze_file_and_save(input_file_path, output_file_path)
