import pandas as pd
from pathlib import Path
import logging
from etl.db_config import load_db_config
from sqlalchemy import create_engine
import traceback


# Logging
from etl.logger import section, timed


# Paths
RAW_DATA_DIR = Path("data_base/raw_data")
CLEANED_DATA_DIR = Path("data_base/cleaned_data")


# Data Loading

def create_data_dir():
    CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Cleaned data directory ready: {CLEANED_DATA_DIR}")


def load_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path.resolve()}")
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {file_name} with shape {df.shape}")
    return df

# Column Normalization


def columns_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase with underscores."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df

# Schema Validation


def validate_schema(df: pd.DataFrame, expected_columns: list) -> None:
    """Validate that DataFrame contains expected columns."""
    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    logging.info("All expected columns are present.")

# Cleaning Functions


def clean_customers() -> pd.DataFrame:
    """Load and clean the customer dataset."""
    customer_df = load_raw_data("olist_customers_dataset.csv")
    logging.info(f"Cleaning customers: {customer_df.shape[0]:,} rows")

    customers = customer_df.copy().drop_duplicates().reset_index(drop=True)
    customers = columns_normalization(customers)

    customers["country"] = "Brazil"

    expected_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "country"
    ]
    try:
        validate_schema(customers, expected_columns)
    except ValueError as e:
        logging.error(f"Schema validation failed: {e}")
        raise

    customers = customers[expected_columns]

    logging.info(f"Cleaned customers: {customers.shape[0]:,} rows")
    logging.info(f"Head of cleaned customers:\n{customers.head()}")
    logging.info(f"Data types in cleaned customers:\n{customers.dtypes}")
    logging.info(
        f"Missing values in cleaned customers:\n{customers.isnull().sum()}")

    return customers


def clean_merchants() -> pd.DataFrame:
    """Load and clean the merchants dataset."""
    merchants_df = load_raw_data("olist_sellers_dataset.csv")
    logging.info(f"Cleaning merchants: {merchants_df.shape[0]:,} rows")

    merchants = merchants_df.drop_duplicates().copy().reset_index(drop=True)
    merchants = columns_normalization(merchants)

    merchants = merchants.rename(columns={"seller_id": "merchant_id"})
    merchants = merchants.rename(
        columns={"seller_zip_code_prefix": "merchant_zip_code_prefix"})
    merchants = merchants.rename(columns={"seller_city": "merchant_city"})
    merchants = merchants.rename(columns={"seller_state": "merchant_state"})

    merchants["country"] = "Brazil"

    expected_columns = [
        "merchant_id",
        "merchant_zip_code_prefix",
        "merchant_city",
        "merchant_state",
        "country"
    ]
    try:
        validate_schema(merchants, expected_columns)
    except ValueError as e:
        logging.error(f"Schema validation failed: {e}")
        raise

    merchants = merchants[expected_columns]

    logging.info(f"Cleaned merchants: {merchants.shape[0]:,} rows")
    logging.info(f"Heading of clean merchants:\n{merchants.head()}")
    logging.info(f"Data types in clean merchants:\n{merchants.dtypes}")
    logging.info(
        f"Missing values in clean merchants:\n{merchants.isnull().sum()}")
    return merchants


def clean_orders() -> pd.DataFrame:
    orders_df = load_raw_data("olist_orders_dataset.csv")
    logging.info(f"Cleaning orders: {orders_df.shape[0]:,} rows")

    orders = orders_df.drop_duplicates().reset_index(drop=True).copy()
    orders = columns_normalization(orders)

    expected_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    try:
        validate_schema(orders, expected_columns)
    except ValueError as e:
        logging.error(f"Schema validation failed: {e}")
        raise

    orders = orders[expected_columns]

    logging.info(f"Cleaned orders: {orders.shape[0]:,} rows")
    logging.info(f"Heading of clean orders:\n{orders.head()}")
    logging.info(f"Data types in clean orders:\n{orders.dtypes}")
    logging.info(f"Missing values in clean orders:\n{orders.isnull().sum()}")

    return orders


def clean_items() -> pd.DataFrame:
    items_df = load_raw_data("olist_order_items_dataset.csv")
    logging.info(f"Cleaning items: {items_df.shape[0]:,} rows")

    items = items_df.drop_duplicates().reset_index(drop=True).copy()
    items = columns_normalization(items)

    items = items.rename(columns={"seller_id": "merchant_id"})

    expected_columns = [
        "order_id",
        "merchant_id",
        "product_id",
        "price",
        "freight_value"
    ]
    try:
        validate_schema(items, expected_columns)
    except ValueError as e:
        logging.error(f"Schema validation failed: {e}")
        raise

    items = items[expected_columns]

    logging.info(f"Cleaned items: {items.shape[0]:,} rows")
    logging.info(f"Heading of clean items:\n{items.head()}")
    logging.info(f"Data types in clean items:\n{items.dtypes}")
    logging.info(f"Missing values in clean items:\n{items.isnull().sum()}")

    return items


def clean_payments() -> pd.DataFrame:
    payments_df = load_raw_data("olist_order_payments_dataset.csv")
    logging.info(f"Cleaning payments: {payments_df.shape[0]:,} rows")

    payments = payments_df.drop_duplicates().reset_index(drop=True).copy()
    payments = columns_normalization(payments)
    expected_columns = [
        "order_id",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]
    try:
        validate_schema(payments, expected_columns)
    except ValueError as e:
        logging.error(f"Schema validation failed: {e}")
        raise

    payments = payments[expected_columns]

    logging.info(f"Cleaned payments: {payments.shape[0]:,} rows")
    logging.info(f"Heading of clean payments:\n{payments.head()}")
    logging.info(f"Data types in clean payments:\n{payments.dtypes}")
    logging.info(
        f"Missing values in clean payments:\n{payments.isnull().sum()}")

    return payments


# Data Merging and Enrichment =  Merging orders, items, and payments into a single transactions dataset for easier analytics and fact table loading.
def transactions_merge(orders: pd.DataFrame, items: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    """Cleaning Txn Data: Merge orders, items, and payments into transactions."""
    logging.info(
        f"Merging transactions: orders={orders.shape[0]:,}, items={items.shape[0]:,}, payments={payments.shape[0]:,}")

    transactions = (
        orders
        .merge(items[["order_id", "merchant_id", "price", "product_id"]], on="order_id", how="left")
        .merge(payments[["order_id", "payment_type", "payment_value"]], on="order_id", how="left")
    )

    logging.info(f"Merged transactions: {transactions.shape[0]:,} rows")
    logging.info(f"Heading of transactions:\n{transactions.head()}")
    logging.info(f"Data types in transactions:\n{transactions.dtypes}")
    logging.info(
        f"Missing values in transactions:\n{transactions.isnull().sum()}")
    return transactions


def handle_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    # There are missing values in critical fields for cancelled orders. These rows cannot be used in any fact table and will be dropped.
    df = df.dropna(subset=[
        "merchant_id",
        "product_id",
        "price",
        "payment_value"
    ]).copy()

    # Fill columns that can safely default
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
        f"Missing values after handling cancellations:\n{df[['merchant_id', 'product_id', 'price', 'payment_value']].isnull().sum()}")

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
def save_dataframe(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame to CSV."""
    output_path = path.with_suffix(".csv")
    df.to_csv(output_path, index=False)
    logging.info(f"Saved CSV: {output_path}")


# Load to PostgreSQL
def load_to_postgres(df: pd.DataFrame, table_name: str, engine) -> None:

    df.to_sql(table_name, engine, schema="staging",
              if_exists="replace", index=False)
    logging.info(f"Loaded {table_name} into PostgreSQL.")


# ETL Pipeline
@timed
def main():

    try:
        section("🚀 STARTING ETL PIPELINE")

        create_data_dir()

        engine = create_engine(load_db_config(), pool_pre_ping=True)

        # Clean data
        section("🧹 CLEANING DATASETS")

        clean_cust = clean_customers()
        clean_merch = clean_merchants()
        clean_ord = clean_orders()
        clean_itm = clean_items()
        clean_pay = clean_payments()

        # Merge
        section("🔗 MERGING TRANSACTIONS")

        transactions = transactions_merge(
            clean_ord,
            clean_itm,
            clean_pay
        )

        # Handle cancellations
        section("🚫 HANDLING CANCELLATIONS")

        transactions = handle_cancellations(transactions)

        # Dates
        section("📅 CONVERTING DATES")

        date_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]

        transactions = convert_date_columns(transactions, date_cols)

        # Save
        section("💾 SAVING CSV OUTPUTS")

        save_dataframe(clean_cust, CLEANED_DATA_DIR / "clean_customers")
        save_dataframe(clean_merch, CLEANED_DATA_DIR / "clean_merchants")
        save_dataframe(transactions, CLEANED_DATA_DIR / "transactions")

        # Load DB
        section("🚚 LOADING TO POSTGRES")

        load_to_postgres(clean_cust, "customers_clean", engine)
        load_to_postgres(clean_merch, "merchants_clean", engine)
        load_to_postgres(transactions, "transactions_clean", engine)

        logging.info("🎉 ETL Pipeline Completed Successfully!")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        logging.error(traceback.format_exc())


# Entry Point
if __name__ == "__main__":
    main()
