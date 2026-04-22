-- Create schema if it doesn't exist 
CREATE SCHEMA IF NOT EXISTS operationals;

-- Customers table
CREATE TABLE IF NOT EXISTS operationals.customers_raw (
    customer_id VARCHAR(255) PRIMARY KEY,
    unique_id VARCHAR(255),
    zip_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

-- Merchants table
CREATE TABLE IF NOT EXISTS operationals.merchants_raw (
    merchant_id VARCHAR(255) PRIMARY KEY,
    zip_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

-- Transactions table
CREATE TABLE IF NOT EXISTS operationals.transactions_raw (
    transaction_id SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    seller_id VARCHAR(255),
    order_status VARCHAR(50),
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
