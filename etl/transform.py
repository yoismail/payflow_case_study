import logging
import pandas as pd
from sqlalchemy import create_engine, text

from etl.db_config import load_db_config
from etl.logger import section, timed

engine = create_engine(load_db_config())


# ---------- STAGING LOAD ----------

def load_staging():
    section("📥 LOADING STAGING TABLES")
    customers = pd.read_sql("SELECT * FROM staging.customers_clean", engine)
    merchants = pd.read_sql("SELECT * FROM staging.merchants_clean", engine)
    transactions = pd.read_sql("SELECT * FROM staging.txns_clean", engine)

    logging.info(f"customers_clean shape: {customers.shape}")
    logging.info(f"merchants_clean shape: {merchants.shape}")
    logging.info(f"txns_clean shape: {transactions.shape}")

    return customers, merchants, transactions


# ---------- ANALYTICS RESET ----------

def reset_analytics_schema():
    section("🧨 RESETTING ANALYTICS SCHEMA")

    stmt = text("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = 'analytics') THEN
                TRUNCATE TABLE 
                    analytics.fact_payments,
                    analytics.fact_order_items,
                    analytics.fact_orders,
                    analytics.dim_customer,
                    analytics.dim_seller,
                    analytics.dim_product,
                    analytics.dim_payment_type,
                    analytics.dim_date
                RESTART IDENTITY CASCADE;
            END IF;
        END $$;
    """)
    with engine.begin() as conn:
        conn.execute(stmt)

    logging.info(
        "Analytics schema reset complete (TRUNCATE + RESTART IDENTITY).")


# ---------- DIMENSION BUILDERS ----------

def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_customer")
    dim_customer = customers.copy()
    dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))
    logging.info(f"dim_customer shape: {dim_customer.shape}")
    return dim_customer


def build_dim_seller(merchants: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_seller")
    dim_seller = merchants.copy()
    dim_seller.insert(0, "seller_key", range(1, len(dim_seller) + 1))
    logging.info(f"dim_seller shape: {dim_seller.shape}")
    return dim_seller


def build_dim_product(transactions: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_product")
    dim_product = (
        transactions[["product_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))
    logging.info(f"dim_product shape: {dim_product.shape}")
    return dim_product


def build_dim_payment_type(transactions: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_payment_type")
    dim_payment_type = (
        transactions[["payment_type"]]
        .fillna("unknown")
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_payment_type.insert(0, "payment_type_key",
                            range(1, len(dim_payment_type) + 1))
    logging.info(f"dim_payment_type shape: {dim_payment_type.shape}")
    return dim_payment_type


def build_dim_date_from_series(dates: pd.Series) -> pd.DataFrame:
    dates = pd.to_datetime(dates, errors="coerce")
    unique_dates = pd.Series(dates.dropna().dt.date.unique()).sort_values()
    dim_date = pd.DataFrame({"full_date": unique_dates})

    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])
    dim_date["date_key"] = dim_date["full_date"].dt.strftime(
        "%Y%m%d").astype(int)

    dim_date["day_of_month"] = dim_date["full_date"].dt.day
    dim_date["month_number"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["quarter_number"] = dim_date["full_date"].dt.quarter
    dim_date["year_number"] = dim_date["full_date"].dt.year
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date["day_of_week_number"] = dim_date["full_date"].dt.weekday + 1
    dim_date["day_of_week_name"] = dim_date["full_date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week_number"].isin([6, 7])

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


def build_dim_date(transactions: pd.DataFrame) -> pd.DataFrame:
    section("🧱 BUILDING dim_date")
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    all_dates = pd.concat(
        [pd.to_datetime(transactions[c], errors="coerce") for c in date_cols],
        axis=0,
    )
    return build_dim_date_from_series(all_dates)


# ---------- HELPERS ----------

def map_date_key(dim_date: pd.DataFrame, ts) -> pd.Series:
    ts = pd.to_datetime(ts, errors="coerce")
    if not isinstance(ts, pd.Series):
        ts = pd.Series(ts)
    ts = ts.dt.date
    date_map = dim_date.set_index("full_date")["date_key"]
    return ts.map(date_map)


def safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([pd.NaT] * len(df))


# ---------- FACT BUILDERS ----------

def build_fact_orders(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_orders")

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

    cust_map = dim_customer.set_index("customer_id")["customer_key"]
    orders["customer_key"] = orders["customer_id"].map(cust_map)

    orders["purchase_date_key"] = map_date_key(
        dim_date, orders["order_purchase_timestamp"])
    orders["approved_date_key"] = map_date_key(
        dim_date, orders["order_approved_at"])
    orders["delivered_carrier_date_key"] = map_date_key(
        dim_date, orders["order_delivered_carrier_date"]
    )
    orders["delivered_customer_date_key"] = map_date_key(
        dim_date, orders["order_delivered_customer_date"]
    )
    orders["estimated_delivery_date_key"] = map_date_key(
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

    fact_orders.insert(0, "fact_order_key", range(1, len(fact_orders) + 1))
    logging.info(f"fact_orders shape: {fact_orders.shape}")
    return fact_orders


def build_fact_order_items(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_seller: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_orders: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_order_items")

    items = transactions.copy()

    cust_map = dim_customer.set_index("customer_id")["customer_key"]
    seller_map = dim_seller.set_index("merchant_id")["seller_key"]
    product_map = dim_product.set_index("product_id")["product_key"]
    order_map = fact_orders.set_index("order_id")["fact_order_key"]

    items["customer_key"] = items["customer_id"].map(cust_map)
    items["seller_key"] = items["seller_id"].map(seller_map)
    items["product_key"] = items["product_id"].map(product_map)
    items["fact_order_key"] = items["order_id"].map(order_map)

    items["purchase_date_key"] = map_date_key(
        dim_date, items["order_purchase_timestamp"]
    )
    items["shipping_limit_date_key"] = map_date_key(
        dim_date, safe_series(items, "shipping_limit_date")
    )

    items["line_total_value"] = items["price"]

    fact_order_items = items[
        [
            "fact_order_key",
            "order_id",
            "customer_key",
            "seller_key",
            "product_key",
            "purchase_date_key",
            "shipping_limit_date_key",
            "order_status",
            "order_purchase_timestamp",
            "price",
            "line_total_value",
        ]
    ].copy()

    fact_order_items.insert(
        0, "fact_order_item_key", range(1, len(fact_order_items) + 1)
    )
    logging.info(f"fact_order_items shape: {fact_order_items.shape}")
    return fact_order_items


def build_fact_payments(
    transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_payment_type: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_orders: pd.DataFrame,
) -> pd.DataFrame:
    section("📊 BUILDING fact_payments")

    payments = transactions.copy()

    cust_map = dim_customer.set_index("customer_id")["customer_key"]
    pay_type_map = dim_payment_type.set_index(
        "payment_type")["payment_type_key"]
    order_map = fact_orders.set_index("order_id")["fact_order_key"]

    payments["customer_key"] = payments["customer_id"].map(cust_map)
    payments["payment_type_key"] = payments["payment_type"].map(pay_type_map)
    payments["fact_order_key"] = payments["order_id"].map(order_map)
    payments["purchase_date_key"] = map_date_key(
        dim_date, payments["order_purchase_timestamp"]
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

    fact_payments.insert(
        0, "fact_payment_key", range(1, len(fact_payments) + 1)
    )
    logging.info(f"fact_payments shape: {fact_payments.shape}")
    return fact_payments


# ---------- WRITE HELPERS ----------

def write_dim_fact(df: pd.DataFrame, table: str):
    df.to_sql(table, engine, schema="analytics",
              if_exists="append", index=False)
    logging.info(f"Loaded {table} into analytics schema.")


# ---------- ORCHESTRATOR ----------

@timed
def run_transform():
    logging.info("🚀 Starting analytics star schema transform...")

    reset_analytics_schema()

    customers, merchants, transactions = load_staging()

    dim_customer = build_dim_customer(customers)
    dim_seller = build_dim_seller(merchants)
    dim_product = build_dim_product(transactions)
    dim_payment_type = build_dim_payment_type(transactions)
    dim_date = build_dim_date(transactions)

    write_dim_fact(dim_customer, "dim_customer")
    write_dim_fact(dim_seller, "dim_seller")
    write_dim_fact(dim_product, "dim_product")
    write_dim_fact(dim_payment_type, "dim_payment_type")
    write_dim_fact(dim_date, "dim_date")

    fact_orders = build_fact_orders(transactions, dim_customer, dim_date)
    write_dim_fact(fact_orders, "fact_orders")

    fact_order_items = build_fact_order_items(
        transactions, dim_customer, dim_seller, dim_product, dim_date, fact_orders
    )
    write_dim_fact(fact_order_items, "fact_order_items")

    fact_payments = build_fact_payments(
        transactions, dim_customer, dim_payment_type, dim_date, fact_orders
    )
    write_dim_fact(fact_payments, "fact_payments")

    logging.info("🎉 Star schema transform completed successfully.")


if __name__ == "__main__":
    run_transform()
