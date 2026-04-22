import logging
import traceback
import time
from pathlib import Path

import pandas as pd

# ----------------------------
# Colored Logging Formatter
# ----------------------------

class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[94m",     # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",    # Red
        "SUCCESS": "\033[92m",  # Green (custom)
        "RESET": "\033[0m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        record.msg = f"{color}{record.msg}{reset}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])


# ----------------------------
# Paths
# ----------------------------
RAW_DATA_DIR = Path("data_base") / "raw_data"
PROCESSED_DATA_DIR = Path("data_base") / "processed_data"
OUTPUT_FORMAT = "csv"


# ----------------------------
# Helper Functions
# ----------------------------

def section(title: str):
    """Print a clean section divider."""
    logging.info("\n" + "=" * 70)
    logging.info(f"🔷 {title}")
    logging.info("=" * 70 + "\n")


def timed(func):
    """Decorator to measure execution time of ETL steps."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logging.info(f"⏱️  Step completed in {elapsed:.2f}s\n")
        return result
    return wrapper


def create_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    logging.info(f"Directory created: {path.resolve()}")


def load_csv(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    logging.info(
        f"Loaded {file_path.name} "
        f"with {df.shape[0]:,} rows "
        f"and {df.shape[1]} columns"
    )
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    logging.info(f"Normalized columns: {list(out.columns)}")
    return out


def convert_date_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
            logging.info(f"Converted to datetime: {col}")
    return out


def save_dataframe(df: pd.DataFrame, output_path: Path, file_format: str = "csv") -> None:
    if file_format == "csv":
        df.to_csv(output_path.with_suffix(".csv"), index=False)
        logging.info(f"Saved CSV: {output_path.with_suffix('.csv')}")
    elif file_format == "parquet":
        df.to_parquet(output_path.with_suffix(".parquet"), index=False)
        logging.info(f"Saved Parquet: {output_path.with_suffix('.parquet')}")
    else:
        raise ValueError(f"Unsupported format: {file_format}")


# ----------------------------
# Cleaning Functions
# ----------------------------

@timed
def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Cleaning customers: {df.shape[0]:,} rows")

    df = normalize_columns(df).copy()

    df = df.rename(columns={
        "unique_id": "customer_unique_id",
        "zip_code_prefix": "customer_zip_code_prefix",
        "city": "customer_city",
        "state": "customer_state",
    })

    required_cols = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(f"Customers dataset missing columns: {missing}")

    df = (
        df[required_cols]
        .drop_duplicates(subset="customer_id")
        .reset_index(drop=True)
    )

    df["country"] = "Brazil"

    logging.info(f"Cleaned customers: {df.shape[0]:,} rows")

    return df


@timed
def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Cleaning sellers: {df.shape[0]:,} rows")

    df = normalize_columns(df).copy()

    required_cols = [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(f"Sellers dataset missing columns: {missing}")

    df = (
        df[required_cols]
        .drop_duplicates(subset="seller_id")
        .rename(columns={
            "seller_id": "merchant_id",
            "seller_zip_code_prefix": "merchant_zip_code_prefix",
            "seller_city": "merchant_city",
            "seller_state": "merchant_state",
        })
        .reset_index(drop=True)
    )
    df["country"] = "Brazil"

    logging.info(f"Cleaned merchants: {df.shape[0]:,} rows")
    return df


@timed
def merge_transactions(orders, items, payments) -> pd.DataFrame:
    logging.info(
        f"Merging transactions: "
        f"orders={orders.shape[0]:,}, "
        f"items={items.shape[0]:,}, "
        f"payments={payments.shape[0]:,}"
    )

    orders = normalize_columns(orders).copy()
    items = normalize_columns(items).copy()
    payments = normalize_columns(payments).copy()

    order_required = [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    item_required = [
        "order_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ]
    payment_required = [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ]

    missing_orders = [c for c in order_required if c not in orders.columns]
    missing_items = [c for c in item_required if c not in items.columns]
    missing_payments = [
        c for c in payment_required if c not in payments.columns]

    if missing_orders:
        raise KeyError(f"Orders dataset missing columns: {missing_orders}")
    if missing_items:
        raise KeyError(f"Items dataset missing columns: {missing_items}")
    if missing_payments:
        raise KeyError(f"Payments dataset missing columns: {missing_payments}")

    clean_orders = orders[order_required].drop_duplicates(subset="order_id")

    transactions = (
        clean_orders
        .merge(
            items[item_required].rename(columns={"seller_id": "merchant_id"}),
            on="order_id",
            how="left",
        )
        .merge(
            payments[payment_required],
            on="order_id",
            how="left",
        )
    )

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
    ]
    transactions = convert_date_columns(transactions, date_columns)

    logging.info(f"Merged transactions: {transactions.shape[0]:,} rows")
    return transactions


# ----------------------------
# ETL Function
# ----------------------------

def run_etl() -> None:
    try:
        section("🚀 STARTING ETL PIPELINE")

        create_dir(PROCESSED_DATA_DIR)

        section("📥 LOADING RAW DATASETS")
        customers = load_csv(RAW_DATA_DIR / "olist_customers_dataset.csv")
        merchants = load_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv")
        items = load_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv")
        orders = load_csv(RAW_DATA_DIR / "olist_orders_dataset.csv")
        payments = load_csv(RAW_DATA_DIR / "olist_order_payments_dataset.csv")

        section("🧹 CLEANING DATASETS")
        clean_customers_df = clean_customers(customers)
        clean_merchants_df = clean_sellers(merchants)

        section("🔗 MERGING TRANSACTIONS")
        transactions_df = merge_transactions(orders, items, payments)

        section("💾 SAVING PROCESSED DATASETS")
        save_dataframe(clean_customers_df, PROCESSED_DATA_DIR /
                       "clean_customers", OUTPUT_FORMAT)
        save_dataframe(clean_merchants_df, PROCESSED_DATA_DIR /
                       "clean_merchants", OUTPUT_FORMAT)
        save_dataframe(transactions_df, PROCESSED_DATA_DIR /
                       "transactions", OUTPUT_FORMAT)

        logging.info(
            "\033[92m🎉 Transformation pipeline completed successfully!\033[0m\n")

    except Exception as exc:
        logging.error(f"Transformation pipeline failed: {exc}")
        logging.error(traceback.format_exc())
        raise


# ----------------------------
# Entry Point
# ----------------------------

if __name__ == "__main__":
    run_etl()
