import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import traceback
import psycopg2


# Logging
from etl.logger import ColorFormatter, section, timed


# Paths
RAW_DATA_DIR = Path("data_base/raw_data")
CLEANED_DATA_DIR = Path("data_base/cleaned_data")

# Environment + DB


def load_env():
    load_dotenv()
    logging.info("Environment variables loaded.")


def get_db_url():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env file")
    logging.info("Database URL retrieved.")
    return db_url

# Data Loading


def create_data_dir():
    if not CLEANED_DATA_DIR.exists():
        CLEANED_DATA_DIR.mkdir()
        logging.info("Data directory created.")
    else:
        logging.info("Data directory already exists.")


def load_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path.resolve()}")
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {file_name} with shape {df.shape}")
    return df

# Cleaning Functions


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer dataset."""
    logging.info(f"Cleaning customers: {df.shape[0]:,} rows")

    clean_customers = df.drop_duplicates(subset="customer_id").copy()
    clean_customers["country"] = "Brazil"
    clean_customers.columns = ["customer_id",
                               "customer_unique_id",
                               "customer_zip_code_prefix",
                               "customer_city",
                               "customer_state",
                               "country"]
    logging.info(f"Cleaned customers: {clean_customers.shape[0]:,} rows")
    logging.info(f"heading of clean customers:\n{clean_customers.head()}")
    logging.info(f"Data types in clean customers:\n{clean_customers.dtypes}")
    logging.info(
        f"Missing values in clean customers:\n{clean_customers.isnull().sum()}")
    return clean_customers


def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sellers dataset."""
    logging.info(f"Cleaning sellers: {df.shape[0]:,} rows")

    clean_merchants = df.drop_duplicates(subset="seller_id").copy()
    clean_merchants["country"] = "Brazil"
    clean_merchants.columns = ["merchant_id", "zip_code",
                               "city", "state", "country"]
    logging.info(f"Cleaned merchants: {clean_merchants.shape[0]:,} rows")
    logging.info(f"heading of clean merchants:\n{clean_merchants.head()}")
    logging.info(f"Data types in clean merchants:\n{clean_merchants.dtypes}")
    logging.info(
        f"Missing values in clean merchants:\n{clean_merchants.isnull().sum()}")
    return clean_merchants


def merge_transactions(orders: pd.DataFrame, items: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    """Cleaning Txn Data: Merge orders, items, and payments into transactions."""
    logging.info(
        f"Merging transactions: orders={orders.shape[0]:,}, items={items.shape[0]:,}, payments={payments.shape[0]:,}")

    clean_orders = orders.drop_duplicates(subset="order_id").copy()
    transactions = (
        clean_orders
        .merge(items[["order_id", "seller_id", "price", "product_id"]], on="order_id", how="left")
        .merge(payments[["order_id", "payment_type", "payment_value"]], on="order_id", how="left")
    )

    logging.info(f"Merged transactions: {transactions.shape[0]:,} rows")
    logging.info(f"heading of transactions:\n{transactions.head()}")
    logging.info(f"Data types in transactions:\n{transactions.dtypes}")
    logging.info(
        f"Missing values in transactions:\n{transactions.isnull().sum()}")
    return transactions


def handle_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values for cancelled  / unavailable txns
    and ensures critical fields are valid for analytics + fact table loading.
    """

    # Drop rows missing critical business keys
    # These rows cannot be used in any fact table
    df = df.dropna(subset=[
        "seller_id",
        "product_id",
        "price",
        "payment_value"
    ])

    # Fill categorical fields that can safely default
    df["payment_type"] = df["payment_type"].fillna("unknown")

    # Create a lifecycle status column
    df["lifecycle_status"] = df.apply(
        lambda row: (
            "cancelled" if row["order_status"] == "canceled"
            else "delivered" if pd.notna(row["order_delivered_customer_date"])
            else "in_transit" if pd.notna(row["order_delivered_carrier_date"])
            else "pending"
        ),
        axis=1
    )

    logging.info(
        f"Handled cancellations, updated lifecycle_status. Sample:\n{df[['order_id', 'order_status', 'lifecycle_status']].head()}")
    # confirm if missing values remain in critical fields after handling cancellations
    logging.info(
        f"Missing values after handling cancellations:\n{df[['seller_id', 'product_id', 'price', 'payment_value']].isnull().sum()}")

    return df


def convert_date_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Convert columns to datetime, ignoring missing columns."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            logging.info(f"Converted column to datetime: {col}")
        else:
            logging.warning(
                f"Column not found, skipping datetime conversion: {col}")

        logging.info(f"Data types after conversion:\n{df.dtypes}")
    return df


# Save Data
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


# Load to PostgreSQL
def load_to_postgres(df, table_name, engine):
    df.to_sql(table_name, engine, schema="operationals",
              if_exists="replace", index=False)
    logging.info(f"Loaded {table_name} into PostgreSQL.")


# ETL Pipeline
@timed
def run_etl():

    try:
        logging.info("Starting ETL pipeline...")

        create_data_dir()

        load_env()
        db_url = get_db_url()
        engine = create_engine(db_url)

        # Load raw data
        section("📥 LOADING RAW DATASETS")
        customers = load_raw_data("olist_customers_dataset.csv")
        merchants = load_raw_data("olist_sellers_dataset.csv")
        items = load_raw_data("olist_order_items_dataset.csv")
        orders = load_raw_data("olist_orders_dataset.csv")
        payments = load_raw_data("olist_order_payments_dataset.csv")

        # Clean
        section("🧹 CLEANING DATASETS")
        clean_cust = clean_customers(customers)
        clean_merchants = clean_sellers(merchants)

        # Merge
        section("🔗 MERGING TRANSACTIONS")
        transactions = merge_transactions(orders, items, payments)

        # Handle cancellations
        section("🚫 HANDLING CANCELLATIONS")
        transactions = handle_cancellations(transactions)

        # Convert dates
        date_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        section("📅 CONVERTING DATE COLUMNS")
        transactions = convert_date_columns(transactions, date_cols)

        # Save processed data
        section("💾 SAVING CLEANED DATASETS")
        save_dataframe(clean_cust, CLEANED_DATA_DIR / "clean_customers")
        save_dataframe(clean_merchants, CLEANED_DATA_DIR / "clean_merchants")
        save_dataframe(transactions, CLEANED_DATA_DIR / "transactions")

        # Load into PostgreSQL
        section("🚚 LOADING CLEANED DATA INTO POSTGRESQL")
        load_to_postgres(clean_cust, "customers_raw", engine)
        load_to_postgres(clean_merchants, "merchants_raw", engine)
        load_to_postgres(transactions, "transactions_raw", engine)

        logging.info(f"\033[92m🎉 ETL pipeline completed successfully!\033[0m")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())


# Entry Point
if __name__ == "__main__":
    run_etl()
