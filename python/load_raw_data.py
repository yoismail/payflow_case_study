
import subprocess
from pathlib import Path
import zipfile
import pandas as pd
import logging
import sys

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Define Paths
# -----------------------------
RAW_DATA_DIR = Path("data_base\\raw_data")
ZIP_FILE = RAW_DATA_DIR / "brazilian-ecommerce.zip"

# -----------------------------
# Step 1: Create Directory
# -----------------------------


def create_data_dir():
    if not RAW_DATA_DIR.exists():
        RAW_DATA_DIR.mkdir()
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")

# -----------------------------
# Step 2: Download Dataset
# -----------------------------


def download_dataset():
    logging.info("Starting dataset download...")

    try:
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "olistbr/brazilian-ecommerce",
            "-p", str(RAW_DATA_DIR)
        ], check=True)

        logging.info("Download completed successfully.")

    except subprocess.CalledProcessError:
        logging.error("Download failed. Check Kaggle API setup.")
        sys.exit(1)

# -----------------------------
# Step 3: Extract Dataset
# -----------------------------


def extract_dataset():
    logging.info("Extracting dataset...")

    if not ZIP_FILE.exists():
        logging.error("Zip file not found. Extraction aborted.")
        sys.exit(1)

    try:
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(RAW_DATA_DIR)

        logging.info("Extraction completed.")

    except zipfile.BadZipFile:
        logging.error("Invalid zip file.")
        sys.exit(1)

# -----------------------------
# Step 4: Validate Data
# -----------------------------


def validate_data():
    logging.info("Validating extracted files...")

    try:
        customers = pd.read_csv(RAW_DATA_DIR / "olist_customers_dataset.csv")
        sellers = pd.read_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv")
        items = pd.read_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv")

        logging.info(f"Customers: {len(customers):,}")
        logging.info(f"Sellers: {len(sellers):,}")
        logging.info(f"Items: {len(items):,}")

    except FileNotFoundError as e:
        logging.error(f"Missing file: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

# -----------------------------
# Pipeline Orchestration
# -----------------------------


def run_pipeline():
    logging.info("ETL Pipeline Started")

    create_data_dir()
    download_dataset()
    extract_dataset()
    validate_data()

    logging.info("ETL Pipeline Completed Successfully")


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run_pipeline()
