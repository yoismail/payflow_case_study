import logging
import pandas as pd
from sqlalchemy import create_engine, text

from etl.db_config import load_db_config
from etl.logger import section, timed


# Database Engine
engine = create_engine(load_db_config(), pool_pre_ping=True)


# STAGING LOADER

def load_staging_tables():
    section("📥 LOADING STAGING TABLES")

    customers = pd.read_sql("SELECT * FROM staging.customers_clean", engine)
    products = pd.read_sql("SELECT * FROM staging.products_clean", engine)
    merchants = pd.read_sql("SELECT * FROM staging.merchants_clean", engine)
    transactions = pd.read_sql(
        "SELECT * FROM staging.transactions_clean", engine)

    logging.info(f"customers_clean shape: {customers.shape}")
    logging.info(f"products_clean shape: {products.shape}")
    logging.info(f"merchants_clean shape: {merchants.shape}")
    logging.info(f"transactions_clean shape: {transactions.shape}")

    return customers, products, merchants, transactions


# Analytics Schema Reset

def truncate_analytics_tables():
    section("🧨 RESETTING ANALYTICS SCHEMA")

    stmt = text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'analytics'
            ) THEN
                TRUNCATE TABLE
                    analytics.fact_payments,
                    analytics.fact_order_items,
                    analytics.fact_orders,
                    analytics.dim_customer,
                    analytics.dim_merchant,
                    analytics.dim_product,
                    analytics.dim_payment_type,
                    analytics.dim_date
                RESTART IDENTITY CASCADE;
            END IF;
        END $$;
    """)

    with engine.begin() as conn:
        conn.execute(stmt)

    logging.info("Analytics schema reset complete.")


# Schema Validation

def validate_schema(df: pd.DataFrame, expected_columns: list) -> None:
    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        logging.error(f"Missing columns: {missing_cols}")
        raise ValueError(f"Missing columns: {missing_cols}")

    logging.info("Schema validation passed.")


# Column Level Cleaning

def clean_text_column(series: pd.Series, case: str = "title") -> pd.Series:
    series = series.astype("string").str.strip()

    if case == "title":
        return series.str.title()

    if case == "upper":
        return series.str.upper()

    if case == "lower":
        return series.str.lower()

    return series


# Dimension Builders

def build_customer_dimension(customers: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_customer")

    expected_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "country"
    ]

    validate_schema(customers, expected_columns)

    dim_customer = customers[expected_columns].copy()

    # Strict key validation
    dim_customer = dim_customer.dropna(subset=["customer_id"])
    dim_customer["customer_id"] = dim_customer["customer_id"].astype(
        "string").str.strip()
    dim_customer["customer_unique_id"] = dim_customer["customer_unique_id"].astype(
        "string").str.strip()

    # Light attribute cleaning
    dim_customer["customer_city"] = clean_text_column(
        dim_customer["customer_city"], "title")
    dim_customer["customer_state"] = clean_text_column(
        dim_customer["customer_state"], "upper")
    dim_customer["country"] = clean_text_column(
        dim_customer["country"], "title")

    # Dimension uniqueness
    dim_customer = (
        dim_customer
        .drop_duplicates(subset=["customer_id"])
        .sort_values("customer_id")
        .reset_index(drop=True)
    )

    # Surrogate key
    dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))

    logging.info(f"dim_customer shape: {dim_customer.shape}")
    return dim_customer


def build_merchant_dimension(merchants: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_merchant")

    expected_columns = [
        "merchant_id",
        "merchant_zip_code_prefix",
        "merchant_city",
        "merchant_state",
        "country"
    ]

    validate_schema(merchants, expected_columns)

    dim_merchant = merchants[expected_columns].copy()

    # Strict key validation
    dim_merchant = dim_merchant.dropna(subset=["merchant_id"])
    dim_merchant["merchant_id"] = dim_merchant["merchant_id"].astype(
        "string").str.strip()

    # Light attribute cleaning
    dim_merchant["merchant_city"] = clean_text_column(
        dim_merchant["merchant_city"], "title")
    dim_merchant["merchant_state"] = clean_text_column(
        dim_merchant["merchant_state"], "upper")
    dim_merchant["country"] = clean_text_column(
        dim_merchant["country"], "title")

    # Dimension uniqueness
    dim_merchant = (
        dim_merchant
        .drop_duplicates(subset=["merchant_id"])
        .sort_values("merchant_id")
        .reset_index(drop=True)
    )

    # Surrogate key
    dim_merchant.insert(0, "merchant_key", range(1, len(dim_merchant) + 1))

    logging.info(f"dim_merchant shape: {dim_merchant.shape}")
    return dim_merchant


def build_product_dimension(products: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_product")

    expected_columns = [
        "product_id",
        "product_category_name"
    ]

    validate_schema(products, expected_columns)

    dim_product = products[expected_columns].copy()

    # Strict key validation
    dim_product = dim_product.dropna(subset=["product_id"])
    dim_product["product_id"] = dim_product["product_id"].astype(
        "string").str.strip()

    # Light attribute cleaning
    dim_product["product_category_name"] = clean_text_column(
        dim_product["product_category_name"], "title"
    ).fillna("Unknown")

    # Dimension uniqueness
    dim_product = (
        dim_product
        .drop_duplicates(subset=["product_id"])
        .sort_values("product_id")
        .reset_index(drop=True)
    )

    # Surrogate key
    dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))

    logging.info(f"dim_product shape: {dim_product.shape}")
    return dim_product


def build_payment_type_dimension(transactions: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_payment_type")

    expected_columns = ["payment_type"]
    validate_schema(transactions, expected_columns)

    dim_payment_type = transactions[["payment_type"]].copy()

    # Clean text
    dim_payment_type["payment_type"] = clean_text_column(
        dim_payment_type["payment_type"],
        "title"
    )

    # Fill missing payment types with Unknown
    dim_payment_type["payment_type"] = dim_payment_type["payment_type"].fillna(
        "Unknown")

    # Dimension uniqueness
    dim_payment_type = (
        dim_payment_type
        .drop_duplicates(subset=["payment_type"])
        .sort_values("payment_type")
        .reset_index(drop=True)
    )

    # Surrogate key
    dim_payment_type.insert(
        0,
        "payment_type_key",
        range(1, len(dim_payment_type) + 1)
    )

    logging.info(f"dim_payment_type shape: {dim_payment_type.shape}")
    return dim_payment_type


def build_date_dimension_from_series(dates: pd.Series) -> pd.DataFrame:
    dates = pd.to_datetime(dates, errors="coerce")

    # Convert all timestamps to dates, remove invalid values, keep only unique dates, and sort them.
    unique_dates = pd.Series(dates.dropna().dt.date.unique()).sort_values()

    dim_date = pd.DataFrame({"full_date": unique_dates})

    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

    # Surrogate key as YYYYMMDD integer
    dim_date["date_key"] = dim_date["full_date"].dt.strftime(
        "%Y%m%d").astype(int)

    # Extract date attributes
    dim_date["day_of_month"] = dim_date["full_date"].dt.day
    dim_date["month_number"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["quarter_number"] = dim_date["full_date"].dt.quarter
    dim_date["year_number"] = dim_date["full_date"].dt.year
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date["day_of_week_number"] = dim_date["full_date"].dt.weekday + 1
    dim_date["day_of_week_name"] = dim_date["full_date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week_number"].isin([6, 7])

    # Reorder columns
    dim_date = dim_date[
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

    logging.info(f"dim_date shape: {dim_date.shape}")
    return dim_date


def build_date_dimension(transactions: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_date")

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
    ]

    # Validate columns first
    validate_schema(transactions, date_cols)

    # Build combined date series
    all_dates = pd.concat(
        [pd.to_datetime(transactions[col], errors="coerce")
         for col in date_cols],
        axis=0
    )

    # Collects all dates from date columns, sends them to the date dimension builder, and returns dim_date.
    return build_date_dimension_from_series(all_dates)


# Resolve date keys by mapping timestamps to the dim_date table.

def resolve_date_key(dim_date: pd.DataFrame, timestamps: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(timestamps, errors="coerce")

    if not isinstance(timestamps, pd.Series):
        timestamps = pd.Series(timestamps)

    # Normalize timestamps so date matching works correctly against dim_date.full_date
    timestamps = timestamps.dt.normalize()

    # Create date lookup: full_date -> date_key
    date_lookup = dim_date.set_index(
        dim_date["full_date"].dt.normalize())["date_key"]

    return timestamps.map(date_lookup)


def get_optional_column(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]

    return pd.Series([pd.NaT] * len(df))


# Fact Builders

def build_orders_fact(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_orders")

    expected_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "lifecycle_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "price",
        "payment_value",
        "product_id",
    ]

    validate_schema(transactions, expected_columns)

    orders = (
        transactions.groupby("order_id")
        .agg(
            {
                "customer_id": "first",
                "order_status": "first",
                "lifecycle_status": "first",
                "order_purchase_timestamp": "first",
                "order_approved_at": "first",
                "order_delivered_carrier_date": "first",
                "order_delivered_customer_date": "first",
                "order_estimated_delivery_date": "first",
                "price": "sum",
                "payment_value": "sum",
                "product_id": pd.Series.nunique,
            }
        )
        .reset_index()
    )

    orders = orders.rename(
        columns={
            "price": "total_item_value",
            "payment_value": "total_payment_value",
            "product_id": "distinct_product_count",
        }
    )

    orders["item_count"] = (
        transactions.groupby("order_id")["product_id"]
        .count()
        .reindex(orders["order_id"])
        .values
    )

    # Resolve customer surrogate key
    customer_lookup = dim_customer.set_index("customer_id")["customer_key"]
    orders["customer_key"] = orders["customer_id"].map(customer_lookup)

    # Resolve date surrogate keys
    orders["purchase_date_key"] = resolve_date_key(
        dim_date, orders["order_purchase_timestamp"]
    )
    orders["approved_date_key"] = resolve_date_key(
        dim_date, orders["order_approved_at"]
    )
    orders["delivered_carrier_date_key"] = resolve_date_key(
        dim_date, orders["order_delivered_carrier_date"]
    )
    orders["delivered_customer_date_key"] = resolve_date_key(
        dim_date, orders["order_delivered_customer_date"]
    )
    orders["estimated_delivery_date_key"] = resolve_date_key(
        dim_date, orders["order_estimated_delivery_date"]
    )

    fact_orders = orders[
        [
            "order_id",
            "customer_key",
            "purchase_date_key",
            "approved_date_key",
            "delivered_carrier_date_key",
            "delivered_customer_date_key",
            "estimated_delivery_date_key",
            "order_status",
            "lifecycle_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "item_count",
            "distinct_product_count",
            "total_item_value",
            "total_payment_value",
        ]
    ].copy()

    # Surrogate key
    fact_orders.insert(0, "fact_order_key", range(1, len(fact_orders) + 1))

    logging.info(f"fact_orders shape: {fact_orders.shape}")
    return fact_orders


def build_order_items_fact(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_merchant: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_orders: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_order_items")

    expected_columns = [
        "order_id",
        "customer_id",
        "merchant_id",
        "product_id",
        "order_status",
        "order_purchase_timestamp",
        "price",
    ]

    validate_schema(transactions, expected_columns)

    items = transactions.copy()

    # Resolve surrogate keys
    customer_lookup = dim_customer.set_index("customer_id")["customer_key"]
    merchant_lookup = dim_merchant.set_index("merchant_id")["merchant_key"]
    product_lookup = dim_product.set_index("product_id")["product_key"]
    order_lookup = fact_orders.set_index("order_id")["fact_order_key"]

    items["customer_key"] = items["customer_id"].map(customer_lookup)
    items["merchant_key"] = items["merchant_id"].map(merchant_lookup)
    items["product_key"] = items["product_id"].map(product_lookup)
    items["fact_order_key"] = items["order_id"].map(order_lookup)

    # Resolve date surrogate keys
    items["purchase_date_key"] = resolve_date_key(
        dim_date,
        items["order_purchase_timestamp"]
    )
    items["shipping_limit_date_key"] = resolve_date_key(
        dim_date,
        get_optional_column(items, "shipping_limit_date")
    )

    items["line_total_value"] = items["price"]

    fact_order_items = items[
        [
            "fact_order_key",
            "order_id",
            "customer_key",
            "merchant_key",
            "product_key",
            "purchase_date_key",
            "shipping_limit_date_key",
            "order_status",
            "order_purchase_timestamp",
            "shipping_limit_date",
            "price",
            "line_total_value",
        ]
    ].copy()

    # Surrogate key
    fact_order_items.insert(
        0,
        "fact_order_item_key",
        range(1, len(fact_order_items) + 1)
    )

    logging.info(f"fact_order_items shape: {fact_order_items.shape}")
    return fact_order_items


def build_payments_fact(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_payment_type: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_orders: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_payments")

    expected_columns = [
        "order_id",
        "customer_id",
        "payment_type",
        "payment_value",
        "order_status",
        "order_purchase_timestamp",
    ]

    validate_schema(transactions, expected_columns)

    payments = transactions.copy()

    # Clean payment_type before mapping so it matches dim_payment_type
    payments["payment_type"] = clean_text_column(
        payments["payment_type"],
        "title"
    ).fillna("Unknown")

    # Resolve surrogate keys
    customer_lookup = dim_customer.set_index("customer_id")["customer_key"]
    payment_type_lookup = dim_payment_type.set_index("payment_type")[
        "payment_type_key"]
    order_lookup = fact_orders.set_index("order_id")["fact_order_key"]

    payments["customer_key"] = payments["customer_id"].map(customer_lookup)
    payments["payment_type_key"] = payments["payment_type"].map(
        payment_type_lookup)
    payments["fact_order_key"] = payments["order_id"].map(order_lookup)

    # Resolve date surrogate key
    payments["purchase_date_key"] = resolve_date_key(
        dim_date,
        payments["order_purchase_timestamp"]
    )

    payments["payment_sequential"] = 1

    fact_payments = payments[
        [
            "fact_order_key",
            "order_id",
            "customer_key",
            "payment_type_key",
            "purchase_date_key",
            "payment_sequential",
            "payment_value",
            "order_status",
            "order_purchase_timestamp",
        ]
    ].copy()

    # Surrogate key
    fact_payments.insert(
        0,
        "fact_payment_key",
        range(1, len(fact_payments) + 1)
    )

    logging.info(f"fact_payments shape: {fact_payments.shape}")
    return fact_payments


# Total rows across all tables

def calculate_total_rows(*dfs) -> int:
    total_rows = sum(df.shape[0] for df in dfs)
    logging.info(f"Total rows across all tables: {total_rows:,}")
    return total_rows


# Write Helpers

def load_to_analytics(df: pd.DataFrame, table: str) -> None:
    df.to_sql(
        table,
        engine,
        schema="analytics",
        if_exists="append",
        index=False
    )

    logging.info(f"Loaded {table} into analytics schema.")


# Orchestrator

@timed
def run_transform():
    logging.info("🚀 Starting analytics star schema transform...")

    truncate_analytics_tables()

    customers, products, merchants, transactions = load_staging_tables()

    dim_customer = build_customer_dimension(customers)
    dim_merchant = build_merchant_dimension(merchants)
    dim_product = build_product_dimension(products)
    dim_payment_type = build_payment_type_dimension(transactions)
    dim_date = build_date_dimension(transactions)

    load_to_analytics(dim_customer, "dim_customer")
    load_to_analytics(dim_merchant, "dim_merchant")
    load_to_analytics(dim_product, "dim_product")
    load_to_analytics(dim_payment_type, "dim_payment_type")
    load_to_analytics(dim_date, "dim_date")

    fact_orders = build_orders_fact(
        transactions,
        dim_customer,
        dim_date
    )
    load_to_analytics(fact_orders, "fact_orders")

    fact_order_items = build_order_items_fact(
        transactions,
        dim_customer,
        dim_merchant,
        dim_product,
        dim_date,
        fact_orders
    )
    load_to_analytics(fact_order_items, "fact_order_items")

    fact_payments = build_payments_fact(
        transactions,
        dim_customer,
        dim_payment_type,
        dim_date,
        fact_orders
    )
    load_to_analytics(fact_payments, "fact_payments")

    calculate_total_rows(
        dim_customer,
        dim_merchant,
        dim_product,
        dim_payment_type,
        dim_date,
        fact_orders,
        fact_order_items,
        fact_payments
    )

    logging.info(
        "🎉\033[92m Star schema transform completed successfully.\033[0m\n")


if __name__ == "__main__":
    run_transform()
