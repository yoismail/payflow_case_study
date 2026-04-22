CREATE SCHEMA IF NOT EXISTS analytics;

-- Drop existing tables
DROP TABLE IF EXISTS analytics.fact_payments CASCADE;
DROP TABLE IF EXISTS analytics.fact_order_items CASCADE;
DROP TABLE IF EXISTS analytics.fact_orders CASCADE;

DROP TABLE IF EXISTS analytics.dim_date CASCADE;
DROP TABLE IF EXISTS analytics.dim_payment_type CASCADE;
DROP TABLE IF EXISTS analytics.dim_product CASCADE;
DROP TABLE IF EXISTS analytics.dim_seller CASCADE;
DROP TABLE IF EXISTS analytics.dim_customer CASCADE;

-- DIMENSIONS

CREATE TABLE analytics.dim_customer (
    customer_key BIGINT PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    country TEXT
);

CREATE TABLE analytics.dim_seller (
    seller_key BIGINT PRIMARY KEY,
    merchant_id TEXT NOT NULL UNIQUE,
    zip_code TEXT,
    city TEXT,
    state TEXT,
    country TEXT
);

CREATE TABLE analytics.dim_product (
    product_key BIGINT PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE
);

CREATE TABLE analytics.dim_payment_type (
    payment_type_key BIGINT PRIMARY KEY,
    payment_type TEXT NOT NULL UNIQUE
);

CREATE TABLE analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter_number INTEGER NOT NULL,
    year_number INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_week_number INTEGER NOT NULL,
    day_of_week_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- FACTS

CREATE TABLE analytics.fact_orders (
    fact_order_key BIGINT PRIMARY KEY,

    order_id TEXT NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL REFERENCES analytics.dim_customer(customer_key),

    purchase_date_key INTEGER REFERENCES analytics.dim_date(date_key),
    approved_date_key INTEGER REFERENCES analytics.dim_date(date_key),
    delivered_carrier_date_key INTEGER REFERENCES analytics.dim_date(date_key),
    delivered_customer_date_key INTEGER REFERENCES analytics.dim_date(date_key),
    estimated_delivery_date_key INTEGER REFERENCES analytics.dim_date(date_key),

    order_status TEXT,
    lifecycle_status TEXT,

    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,

    item_count INTEGER,
    distinct_product_count INTEGER,

    total_item_value NUMERIC(14,2),
    total_payment_value NUMERIC(14,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.fact_order_items (
    fact_order_item_key BIGINT PRIMARY KEY,

    fact_order_key BIGINT REFERENCES analytics.fact_orders(fact_order_key),

    order_id TEXT NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES analytics.dim_customer(customer_key),
    seller_key BIGINT REFERENCES analytics.dim_seller(seller_key),
    product_key BIGINT REFERENCES analytics.dim_product(product_key),

    purchase_date_key INTEGER REFERENCES analytics.dim_date(date_key),
    shipping_limit_date_key INTEGER REFERENCES analytics.dim_date(date_key),

    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    shipping_limit_date TIMESTAMP,

    price NUMERIC(14,2),
    line_total_value NUMERIC(14,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.fact_payments (
    fact_payment_key BIGINT PRIMARY KEY,

    fact_order_key BIGINT REFERENCES analytics.fact_orders(fact_order_key),

    order_id TEXT NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES analytics.dim_customer(customer_key),
    payment_type_key BIGINT REFERENCES analytics.dim_payment_type(payment_type_key),

    purchase_date_key INTEGER REFERENCES analytics.dim_date(date_key),

    payment_sequential INTEGER,
    payment_value NUMERIC(14,2),

    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INDEXES

CREATE INDEX idx_dim_customer_customer_id ON analytics.dim_customer(customer_id);
CREATE INDEX idx_dim_seller_merchant_id ON analytics.dim_seller(merchant_id);
CREATE INDEX idx_dim_product_product_id ON analytics.dim_product(product_id);
CREATE INDEX idx_dim_payment_type_payment_type ON analytics.dim_payment_type(payment_type);

CREATE INDEX idx_fact_orders_customer_key ON analytics.fact_orders(customer_key);
CREATE INDEX idx_fact_orders_purchase_date_key ON analytics.fact_orders(purchase_date_key);

CREATE INDEX idx_fact_order_items_order_id ON analytics.fact_order_items(order_id);
CREATE INDEX idx_fact_order_items_customer_key ON analytics.fact_order_items(customer_key);
CREATE INDEX idx_fact_order_items_product_key ON analytics.fact_order_items(product_key);
CREATE INDEX idx_fact_order_items_seller_key ON analytics.fact_order_items(seller_key);

CREATE INDEX idx_fact_payments_order_id ON analytics.fact_payments(order_id);
CREATE INDEX idx_fact_payments_customer_key ON analytics.fact_payments(customer_key);
CREATE INDEX idx_fact_payments_payment_type_key ON analytics.fact_payments(payment_type_key);
CREATE INDEX idx_fact_payments_purchase_date_key ON analytics.fact_payments(purchase_date_key);

COMMIT;
