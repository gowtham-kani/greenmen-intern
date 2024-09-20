import pandas as pd
import time
from pathlib import Path
from loguru import logger

# Configure the logger
logger.add("file_{time}.log", rotation="1 MB", level="INFO", format="{time} {level} {message}")

# Function to read a file (either CSV or Parquet)
def read_file(file_path):
    if file_path.suffix == '.csv':
        df = pd.read_csv(file_path)
    elif file_path.suffix == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Parquet file.")
    
    return df

# Function to perform data quality checks for each column
def check_data_quality(df):
    data_quality = []
    total_records = len(df)  # Total number of records in the DataFrame

    for column in df.columns:
        start_time = time.time()

        col_data = df[column]

        # Data type of the column
        data_type = col_data.dtype

        # Null values
        null_count = col_data.isnull().sum()

        # Null string values (empty strings or strings with only whitespace)
        null_string_count = col_data.apply(lambda x: isinstance(x, str) and x.strip() == '').sum()
        
        # Non-null and non-empty data for duplicates and unique counts
        non_null_data = col_data.dropna().apply(lambda x: x.strip() if isinstance(x, str) else x)
        non_null_data = non_null_data[non_null_data != '']  # Filter out empty strings for duplicates and unique counts

        # Duplicate values in the column (excluding nulls and null strings)
        duplicate_count = non_null_data.duplicated(keep=False).sum()

        # Unique value count (excluding nulls and empty strings)
        unique_count = non_null_data.nunique(dropna=True)

        time_taken = time.time() - start_time

        # Calculate valid records (i.e., non-null, non-duplicate, non-empty)
        valid_records = total_records - null_count - duplicate_count - null_string_count

        

        # Store the data quality info for each column in a list
        data_quality.append({
            'Column': column,
            'Null Values': null_count,
            'Null String Values': null_string_count,
            'Duplicate Values': duplicate_count,
            'Unique Values': unique_count,
            'Data Type': data_type,
            'Total Records': total_records,
            'Time Taken (seconds)': round(time_taken, 10),
           
        })

    return pd.DataFrame(data_quality)

# Function to save the data quality report to a CSV file
def save_data_quality_to_csv(data_quality_df, output_file):
    data_quality_df.to_csv(output_file, index=False)
    logger.info(f"Data quality report saved to {output_file}")

# Main function to execute the steps
def analyze_file_and_save(input_file_path, output_file_path):
    input_file_path = Path(input_file_path)  # Convert to Path object for easier manipulation

    if not input_file_path.is_file():
        raise FileNotFoundError(f"The file {input_file_path} does not exist.")
    
    df = read_file(input_file_path)
    data_quality_df = check_data_quality(df)
    save_data_quality_to_csv(data_quality_df, output_file_path)

# Example usage (replace with actual file paths)
input_file_path = '/Users/vale.muthu/greenmen-intern/csv_input_file.csv'  # Or 'your_file.parquet'
output_file_path = '/Users/vale.muthu/greenmen-intern/csv_output_file.csv'  # CSV file for the data quality report

analyze_file_and_save(input_file_path, output_file_path)
