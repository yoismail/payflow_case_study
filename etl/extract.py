
import subprocess
from pathlib import Path
import zipfile
import pandas as pd
import logging
import sys

# Logging Configuration
from etl.logging import ColorFormatter, section, timed

# Define Directory Paths
RAW_DATA_DIR = Path("data_base\\raw_data")
ZIP_FILE = RAW_DATA_DIR / "brazilian-ecommerce.zip"

# Create Directory if not exists


def create_data_dir():
    if not RAW_DATA_DIR.exists():
        RAW_DATA_DIR.mkdir()
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")

# Download Dataset from Kaggle


def download_dataset():
    logging.info("Starting dataset download...")

    try:
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "olistbr/brazilian-ecommerce",
            "-p", str(RAW_DATA_DIR)
        ], check=True)

        logging.info(f"\033[92m🎉 Download completed successfully.\033[0m\n")

    except subprocess.CalledProcessError:
        logging.error("Download failed. Check Kaggle API setup.")
        sys.exit(1)


# Extract Dataset
def extract_dataset():
    logging.info("Extracting dataset...")

    if not ZIP_FILE.exists():
        logging.error("Zip file not found. Extraction aborted.")
        sys.exit(1)

    try:
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(RAW_DATA_DIR)

        logging.info(f"\033[92m🎉 Extraction completed successfully.\033[0m\n")

    except zipfile.BadZipFile:
        logging.error("Invalid zip file.")
        sys.exit(1)

# Validate Extracted Data


def validate_data():
    logging.info("Validating extracted files...")

    try:
        customers = pd.read_csv(RAW_DATA_DIR / "olist_customers_dataset.csv")
        sellers = pd.read_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv")
        items = pd.read_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv")
        products = pd.read_csv(RAW_DATA_DIR / "olist_products_dataset.csv")
        categories = pd.read_csv(
            RAW_DATA_DIR / "product_category_name_translation.csv")
        reviews = pd.read_csv(RAW_DATA_DIR / "olist_order_reviews_dataset.csv")
        sellers = pd.read_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv")
        orders = pd.read_csv(RAW_DATA_DIR / "olist_orders_dataset.csv")
        geo_location = pd.read_csv(
            RAW_DATA_DIR / "olist_geolocation_dataset.csv")

        logging.info(f"Customers:{len(customers):,}")
        logging.info(f"Sellers:{len(sellers):,}")
        logging.info(f"Items:{len(items):,}")
        logging.info(f"Products:{len(products):,}")
        logging.info(f"Categories:{len(categories):,}")
        logging.info(f"Reviews:{len(reviews):,}")
        logging.info(f"Orders:{len(orders):,}")
        logging.info(f"Geo Location:{len(geo_location):,}")

        logging.info(
            f"\033[92m🎉 Data validation completed successfully.\033[0m\n")

    except FileNotFoundError as e:
        logging.error(f"Missing file: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

# Pipeline Orchestration


@timed
def run_pipeline():
    section("🚀 STARTING EXTRACT PIPELINE")

    create_data_dir()
    download_dataset()
    extract_dataset()
    validate_data()

    logging.info(
        f"\033[92m🎉 Pipeline completed successfully.\033[0m\n")


# Entry Point
if __name__ == "__main__":
    run_pipeline()
