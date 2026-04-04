from __future__ import annotations

from pathlib import Path
import logging
import os
import sys
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging


# =========================================================
# LOGGING
# =========================================================

class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[94m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "SUCCESS": "\033[92m",
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
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


def section(title: str):
    logging.info("\n" + "=" * 70)
    logging.info(f"🔷 {title}")
    logging.info("=" * 70 + "\n")


def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logging.info(f"⏱️  Step completed in {elapsed:.2f}s\n")
        return result
    return wrapper


# =========================================================
# CONFIG
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / "data_base" / "processed_data"

CUSTOMER_FILE_FORMATS = ["clean_customers.csv"]
# kept for compatibility, not used
MERCHANT_FILE_FORMATS = ["clean_merchants.csv"]
TRANSACTION_FILE_FORMATS = ["transactions.csv"]


# =========================================================
# HELPERS
# =========================================================

def load_env():
    load_dotenv()
    logging.info("Environment variables loaded.")


def get_db_url():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env file")
    logging.info("Database URL retrieved.")
    return db_url


def find_existing_file(directory: Path, candidates: list[str]) -> Path:
    for candidate in candidates:
        path = directory / candidate
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not find any matching file in {directory}. Tried: {candidates}")


def read_dataset(path: Path) -> pd.DataFrame:
    logging.info(f"Reading dataset: {path}")
    df = pd.read_csv(path)
    logging.info(f"Loaded {path.name} with shape {df.shape}")
    return df


# =========================================================
# DIMENSIONS
# =========================================================

def make_date_key(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    dim = customers.copy()

    dim = dim.rename(columns={
        "customer_unique_id": "customer_unique_id",
        "customer_zip_code_prefix": "customer_zip_code_prefix",
        "customer_city": "customer_city",
        "customer_state": "customer_state",
    })

    dim = dim.drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
    dim.insert(0, "customer_key", range(1, len(dim) + 1))

    return dim[
        [
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "country",
        ]
    ]


def build_dim_product(transactions: pd.DataFrame) -> pd.DataFrame:
    if "product_id" not in transactions.columns:
        raise KeyError("transactions is missing required column: 'product_id'")
    dim = (
        transactions[["product_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.insert(0, "product_key", range(1, len(dim) + 1))
    return dim


def build_dim_payment_type(transactions: pd.DataFrame) -> pd.DataFrame:
    if "payment_type" not in transactions.columns:
        raise KeyError(
            "transactions is missing required column: 'payment_type'")
    dim = (
        transactions[["payment_type"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.insert(0, "payment_type_key", range(1, len(dim) + 1))
    return dim


def build_dim_date(transactions: pd.DataFrame) -> pd.DataFrame:
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    dates = []
    for col in date_columns:
        if col in transactions.columns:
            dates.append(
                pd.to_datetime(transactions[col],
                               errors="coerce").dt.normalize()
            )

    if not dates:
        raise ValueError(
            "No valid date columns found in transactions for dim_date.")

    combined = (
        pd.concat(dates)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    dim = pd.DataFrame({"full_date": combined})
    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["day_of_month"] = dim["full_date"].dt.day
    dim["month_number"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["quarter_number"] = dim["full_date"].dt.quarter
    dim["year_number"] = dim["full_date"].dt.year
    dim["week_of_year"] = dim["full_date"].dt.isocalendar().week.astype(int)
    dim["day_of_week_number"] = dim["full_date"].dt.isocalendar().day.astype(int)
    dim["day_of_week_name"] = dim["full_date"].dt.day_name()
    dim["is_weekend"] = dim["day_of_week_number"].isin([6, 7])

    return dim[
        [
            "date_key",
            "full_date",
            "day_of_month",
            "month_number",
            "month_name",
            "quarter_number",
            "year_number",
            "week_of_year",
            "day_of_week_number",
            "day_of_week_name",
            "is_weekend",
        ]
    ]


# =========================================================
# FACTS
# =========================================================

def build_fact_orders(transactions: pd.DataFrame, dim_customer: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "product_id",
        "price",
        "payment_value",
    ]
    missing = [c for c in required_cols if c not in transactions.columns]
    if missing:
        raise KeyError(
            f"transactions is missing required columns for fact_orders: {missing}")

    order_level = (
        transactions.groupby(["order_id", "customer_id"], dropna=False)
        .agg(
            order_status=("order_status", "first"),
            order_purchase_timestamp=("order_purchase_timestamp", "min"),
            order_approved_at=("order_approved_at", "min"),
            order_delivered_carrier_date=(
                "order_delivered_carrier_date", "min"),
            order_delivered_customer_date=(
                "order_delivered_customer_date", "min"),
            order_estimated_delivery_date=(
                "order_estimated_delivery_date", "min"),
            item_count=("order_id", "size"),
            distinct_product_count=("product_id", "nunique"),
            total_item_value=("price", "sum"),
            total_payment_value=("payment_value", "sum"),
        )
        .reset_index()
    )

    order_level["total_item_value"] = order_level["total_item_value"].fillna(0)
    order_level["total_payment_value"] = order_level["total_payment_value"].fillna(
        0)
    order_level["total_order_value"] = order_level["total_item_value"]

    fact = order_level.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="inner",
    )

    fact["purchase_date_key"] = make_date_key(fact["order_purchase_timestamp"])
    fact["approved_date_key"] = make_date_key(fact["order_approved_at"])
    fact["delivered_carrier_date_key"] = make_date_key(
        fact["order_delivered_carrier_date"]
    )
    fact["delivered_customer_date_key"] = make_date_key(
        fact["order_delivered_customer_date"]
    )
    fact["estimated_delivery_date_key"] = make_date_key(
        fact["order_estimated_delivery_date"]
    )

    return fact[
        [
            "order_id",
            "customer_key",
            "purchase_date_key",
            "approved_date_key",
            "delivered_carrier_date_key",
            "delivered_customer_date_key",
            "estimated_delivery_date_key",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "item_count",
            "distinct_product_count",
            "total_item_value",
            "total_order_value",
            "total_payment_value",
        ]
    ]


def build_fact_order_items(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    required_cols = [
        "order_id",
        "customer_id",
        "product_id",
        "order_status",
        "order_purchase_timestamp",
        "price",
    ]
    missing = [c for c in required_cols if c not in transactions.columns]
    if missing:
        raise KeyError(
            f"transactions is missing required columns for fact_order_items: {missing}")

    fact = transactions.copy()

    fact = fact.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="inner",
    )
    fact = fact.merge(
        dim_product[["product_key", "product_id"]],
        on="product_id",
        how="left",
    )

    fact["purchase_date_key"] = make_date_key(fact["order_purchase_timestamp"])
    fact["line_total_value"] = fact["price"].fillna(0)

    return fact[
        [
            "order_id",
            "customer_key",
            "product_key",
            "purchase_date_key",
            "order_status",
            "order_purchase_timestamp",
            "price",
            "line_total_value",
        ]
    ]


def build_fact_payments(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_payment_type: pd.DataFrame,
) -> pd.DataFrame:
    required_cols = [
        "order_id",
        "customer_id",
        "payment_type",
        "payment_value",
        "order_status",
        "order_purchase_timestamp",
    ]

    missing = [c for c in required_cols if c not in transactions.columns]
    if missing:
        raise KeyError(
            f"transactions is missing required columns for fact_payments: {missing}")

    fact = transactions[transactions["payment_value"].notna()].copy()

    fact = fact.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="inner",
    )
    fact = fact.merge(
        dim_payment_type[["payment_type_key", "payment_type"]],
        on="payment_type",
        how="left",
    )

    fact["purchase_date_key"] = make_date_key(fact["order_purchase_timestamp"])

    return fact[
        [
            "order_id",
            "customer_key",
            "payment_type_key",
            "purchase_date_key",
            "payment_value",
            "order_status",
            "order_purchase_timestamp",
        ]
    ]


# =========================================================
# CREATE ANALYTICS TABLES
# =========================================================

@timed
def create_analytics_tables(engine) -> None:
    section("🏗️ CREATING ANALYTICS TABLES")

    sql_path = PROJECT_ROOT / "sql" / "create_analytics_tables.sql"

    if not sql_path.exists():
        raise FileNotFoundError(f"DDL file not found: {sql_path}")

    logging.info(f"Loading DDL from: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        ddl_content = f.read()

    statements = [
        stmt.strip()
        for stmt in ddl_content.split(";")
        if stmt.strip()
    ]

    logging.info(f"Executing {len(statements)} DDL statements...")

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    logging.info(
        "\033[92mAnalytics schema and tables created successfully.\033[0m")


# =========================================================
# WRITE TABLES
# =========================================================

@timed
def load_to_postgres(df: pd.DataFrame, table_name: str, engine) -> None:
    logging.info(f"Loading {len(df):,} rows into analytics.{table_name}")
    df.to_sql(
        name=table_name,
        con=engine,
        schema="analytics",
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi",
    )
    logging.info(f"Loaded {table_name} into PostgreSQL.")


# =========================================================
# MAIN
# =========================================================

def main() -> None:
    section("🚀 STARTING ANALYTICS LOAD")

    load_env()
    db_url = get_db_url()
    engine = create_engine(db_url)

    section("📥 LOADING PROCESSED DATASETS")
    customer_path = find_existing_file(
        PROCESSED_DATA_DIR, CUSTOMER_FILE_FORMATS)
    # merchant_path kept for compatibility, but not used
    _ = find_existing_file(PROCESSED_DATA_DIR, MERCHANT_FILE_FORMATS)
    transaction_path = find_existing_file(
        PROCESSED_DATA_DIR, TRANSACTION_FILE_FORMATS)

    customers = read_dataset(customer_path)
    transactions = read_dataset(transaction_path)

    logging.info(f"Customers shape: {customers.shape}")
    logging.info(f"Transactions shape: {transactions.shape}")

    section("📐 BUILDING DIMENSIONS")
    dim_customer = build_dim_customer(customers)
    dim_product = build_dim_product(transactions)
    dim_payment_type = build_dim_payment_type(transactions)
    dim_date = build_dim_date(transactions)

    section("📊 BUILDING FACT TABLES")
    fact_orders = build_fact_orders(transactions, dim_customer)
    fact_order_items = build_fact_order_items(
        transactions, dim_customer, dim_product)
    fact_payments = build_fact_payments(
        transactions, dim_customer, dim_payment_type)

    section("🏗️ INITIALIZING ANALYTICS SCHEMA")
    create_analytics_tables(engine)

    section("📤 LOADING TABLES INTO WAREHOUSE")
    load_to_postgres(dim_customer, "dim_customer", engine)
    load_to_postgres(dim_product, "dim_product", engine)
    load_to_postgres(dim_payment_type, "dim_payment_type", engine)
    load_to_postgres(dim_date, "dim_date", engine)

    load_to_postgres(fact_orders, "fact_orders", engine)
    load_to_postgres(fact_order_items, "fact_order_items", engine)
    load_to_postgres(fact_payments, "fact_payments", engine)

    logging.info(
        "\033[92m🎉 Analytics warehouse loaded successfully from processed data.\033[0m\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.exception(f"Analytics load failed: {exc}")
        sys.exit(1)
