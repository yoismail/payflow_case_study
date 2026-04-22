import pandas as pd
from pathlib import Path
import logging

# Logging setup
from etl.logging import ColorFormatter, section, timed


@timed
def main():
    # Dynamic File Discovery
    RAW_DATA_DIR = Path("data_base") / "raw_data"

    if not RAW_DATA_DIR.exists():
        logging.error(
            f"Raw data directory does not exist: {RAW_DATA_DIR.resolve()}")
        return  # Exit the function if directory doesn't exist

    logging.info(f"Searching for CSV files in: {RAW_DATA_DIR.resolve()}")
    files_to_process = list(RAW_DATA_DIR.glob(
        "*.csv"))  # Get a list of Path objects

    if not files_to_process:
        logging.warning(
            f"No CSV files found in {RAW_DATA_DIR}. Nothing to process.")
        return  # Exit if no files found

    logging.info(f"Found {len(files_to_process)} CSV files to process.")

    # Store summary info
    summary_list = []

    # Load and explore datasets
    for file_path in files_to_process:
        file_name = file_path.name
        try:
            df = pd.read_csv(file_path)
            logging.info(f"Exploring {file_name}...")
            logging.info(f"Shape of {file_name}: {df.shape}")
            logging.info(f"Columns in {file_name}: {df.columns.tolist()}")
            logging.info(f"Data types in {file_name}:\n{df.dtypes}")
            logging.info(
                f"Missing values in {file_name}:\n{df.isnull().sum()}")

            logging.info(
                f"\033[92m🎉 Data exploration completed for {file_name}.\033[0m\n")

            # Collect summary info
            summary_list.append({
                "File": file_name,
                "Rows": df.shape[0],
                "Columns": df.shape[1],
                "Total Nulls": df.isnull().sum().sum()
            })

        except FileNotFoundError:
            logging.error(
                f"File {file_name} not found during read. Please check the path and try again.")
        except pd.errors.EmptyDataError:
            logging.warning(f"File {file_name} is empty. Skipping.")
        except Exception as e:
            logging.error(f"Error reading file {file_name}: {e}")

    # Create and display summary table
    if summary_list:
        summary_df = pd.DataFrame(summary_list)
        logging.info("\n=== Summary Table ===")
        # Using to_string for better console formatting of the DataFrame
        logging.info(f"\n{summary_df.to_string(index=False)}\n")
    else:
        logging.info(
            "No data was successfully processed to create a summary table.")


# Main entry point
if __name__ == "__main__":

    main()
