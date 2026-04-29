-- Create schema if it doesn't exist 
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.customers_clean CASCADE;
DROP TABLE IF EXISTS staging.merchants_clean CASCADE;
DROP TABLE IF EXISTS staging.products_clean CASCADE;
DROP TABLE IF EXISTS staging.orders_clean CASCADE;
DROP TABLE IF EXISTS staging.items_clean CASCADE;
DROP TABLE IF EXISTS staging.transactions_clean CASCADE;


-- Customers table
CREATE TABLE IF NOT EXISTS staging.customers_clean (
    customer_id VARCHAR(255) PRIMARY KEY,
    unique_id VARCHAR(255),
    zip_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

-- Merchants table
CREATE TABLE IF NOT EXISTS staging.merchants_clean (
    merchant_id VARCHAR(255) PRIMARY KEY,
    zip_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

-- Products table
CREATE TABLE IF NOT EXISTS staging.products_clean (
    product_id VARCHAR(255) PRIMARY KEY,
    product_category_name VARCHAR(255)
);


-- Orders table
CREATE TABLE IF NOT EXISTS staging.orders_clean (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(50),
    order_lifecycle_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);


-- Items table
CREATE TABLE IF NOT EXISTS staging.items_clean (
    order_id VARCHAR(255),
    product_id VARCHAR(255),
    seller_id VARCHAR(255),
    price INTEGER,
    payment_type VARCHAR(50),
    payment_value DECIMAL(10, 2)
);

-- Transactions table
CREATE TABLE IF NOT EXISTS staging.transactions_clean (
    transaction_id SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    seller_id VARCHAR(255),
    order_status VARCHAR(50),
    order_lifecycle_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price INTEGER,
    payment_type VARCHAR(50),
    payment_value DECIMAL(10, 2)
);

COMMIT;
