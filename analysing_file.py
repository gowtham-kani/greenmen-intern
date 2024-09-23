import pandas as pd
import time
import sys
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

# Main handler function (replaces lambda_handler)
def process_data(event):
    input_file_path = event["input_file_path"]
    output_file_path = event["output_file_path"]

    # Validate input file
    file_path = Path(input_file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Read the file and perform data quality checks
    df = read_file(file_path)
    data_quality_df = check_data_quality(df)

    # Save the data quality report to CSV
    save_data_quality_to_csv(data_quality_df, output_file_path)

    # Example return statement (customize based on your needs)
    return {"status": "success", "output_file": output_file_path}

# Main function to parse command-line arguments and run the process
def main():
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <input_file_path> <output_file_path>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    # Create event dictionary to simulate lambda event
    event = {
        "input_file_path": input_file_path,
        "output_file_path": output_file_path
    }

    # Run the process
    result = process_data(event)
    print(f"Process completed: {result}")

# Entry point for script execution
if __name__ == "__main__":
    main()





