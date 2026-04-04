import pandas as pd
import logging
from pathlib import Path
import traceback

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Paths
# ----------------------------
RAW_DATA_DIR = Path("data_base") / "raw_data"
PROCESSED_DATA_DIR = Path("data_base") / "processed_data"
OUTPUT_FORMAT = "csv"

# ----------------------------
# Helper Functions
# ----------------------------


def create_dir(path: Path):
    """Create directory if it does not exist."""
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Directory created: {path.resolve()}")
    else:
        logging.info(f"Directory already exists: {path.resolve()}")


def load_csv(file_path: Path) -> pd.DataFrame:
    """Load CSV and log shape."""
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(
        f"Loaded {file_path.name} with {df.shape[0]:,} rows and {df.shape[1]} columns")
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer dataset."""
    logging.info(f"Cleaning customers: {df.shape[0]:,} rows")

    clean_customers = df.drop_duplicates(subset="customer_id").copy()
    clean_customers["country"] = "Brazil"
    clean_customers.columns = ["customer_id", "unique_id", "zip_code",
                               "city", "state", "country"]
    logging.info(f"Cleaned customers: {clean_customers.shape[0]:,} rows")
    return clean_customers


def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sellers dataset."""
    logging.info(f"Cleaning sellers: {df.shape[0]:,} rows")

    clean_merchants = df.drop_duplicates(subset="seller_id").copy()
    clean_merchants["country"] = "Brazil"
    clean_merchants.columns = ["merchant_id", "zip_code",
                               "city", "state", "country"]
    logging.info(f"Cleaned merchants: {clean_merchants.shape[0]:,} rows")
    return clean_merchants


def merge_transactions(orders: pd.DataFrame, items: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    """Cleaning Txn Data: Merge orders, items, and payments into transactions."""
    logging.info(
        f"Merging transactions: orders={orders.shape[0]:,}, items={items.shape[0]:,}, payments={payments.shape[0]:,}")

    clean_orders = orders.drop_duplicates(subset="order_id").copy()
    transactions = (
        clean_orders
        .merge(items[["order_id", "seller_id", "price"]], on="order_id", how="left")
        .merge(payments[["order_id", "payment_type", "payment_value"]], on="order_id", how="left")
    )
    logging.info(f"Merged transactions: {transactions.shape[0]:,} rows")
    return transactions


def convert_date_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Convert columns to datetime, ignoring missing columns."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            logging.info(f"Converted column to datetime: {col}")
        else:
            logging.warning(
                f"Column not found, skipping datetime conversion: {col}")
    return df


def save_dataframe(df: pd.DataFrame, path: Path, format: str = "csv"):
    """Save DataFrame to CSV or Parquet."""
    if format == "csv":
        df.to_csv(path.with_suffix(".csv"), index=False)
        logging.info(f"Saved CSV: {path.with_suffix('.csv')}")
    elif format == "parquet":
        df.to_parquet(path.with_suffix(".parquet"), index=False)
        logging.info(f"Saved Parquet: {path.with_suffix('.parquet')}")
    else:
        logging.error(f"Unsupported format: {format}")
        raise ValueError(f"Unsupported format: {format}")


# ----------------------------
# ETL Function
# ----------------------------
def run_etl():
    try:
        logging.info("Starting ETL pipeline...")

        # Ensure output directory exists
        create_dir(PROCESSED_DATA_DIR)

        # Load raw datasets
        customers = load_csv(RAW_DATA_DIR / "olist_customers_dataset.csv")
        merchants = load_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv")
        items = load_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv")
        orders = load_csv(RAW_DATA_DIR / "olist_orders_dataset.csv")
        payments = load_csv(RAW_DATA_DIR / "olist_order_payments_dataset.csv")

        # Clean datasets
        clean_cust = clean_customers(customers)
        clean_merchants = clean_sellers(merchants)

        # Merge transactions
        transactions = merge_transactions(orders, items, payments)

        # Convert date columns
        date_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        transactions = convert_date_columns(transactions, date_columns)

        # Save cleaned datasets
        save_dataframe(clean_cust, PROCESSED_DATA_DIR /
                       "clean_customers", OUTPUT_FORMAT)
        save_dataframe(clean_merchants, PROCESSED_DATA_DIR /
                       "clean_merchants", OUTPUT_FORMAT)
        save_dataframe(transactions, PROCESSED_DATA_DIR /
                       "transactions", OUTPUT_FORMAT)

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


# ----------------------------
# Entry Point
# ----------------------------
if __name__ == "__main__":
    run_etl()
