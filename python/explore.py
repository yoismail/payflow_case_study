import pandas as pd
from pathlib import Path
import logging

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Paths and file list
# ----------------------------
RAW_DATA_DIR = Path("data_base") / "raw_data"

files = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
]

if not RAW_DATA_DIR.exists():
    logging.error(
        f"Raw data directory does not exist: {RAW_DATA_DIR.resolve()}")

logging.info("Data is available. Exploring now...")

# ----------------------------
# Store summary info
# ----------------------------
summary_list = []

# ----------------------------
# Load and explore datasets
# ----------------------------
for file_name in files:
    file_path = RAW_DATA_DIR / file_name
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Exploring {file_name}...")
        logging.info(f"Shape of {file_name}: {df.shape}")
        logging.info(f"Columns in {file_name}: {df.columns.tolist()}")
        logging.info(f"Data types in {file_name}:\n{df.dtypes}")
        logging.info(
            f"Missing values in {file_name}:\n{df.isnull().sum()}")

        logging.info(f"Data Exploration completed for {file_name}.\n")

        # Collect summary info
        summary_list.append({
            "File": file_name,
            "Rows": df.shape[0],
            "Columns": df.shape[1],
            "Total Nulls": df.isnull().sum().sum()
        })

    except FileNotFoundError:
        logging.error(
            f"File {file_name} not found. Please check the path and try again.")

# ----------------------------
# Create and display summary table
# ----------------------------
if summary_list:
    summary_df = pd.DataFrame(summary_list)
    logging.info("\n=== Summary Table ===")
    logging.info(f"\n{summary_df.to_string(index=False)}")
