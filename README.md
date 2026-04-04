# 📦 Payflow Case Study - End‑to‑End ETL & Analytics Pipeline

This project implements a **production‑grade ETL and analytics pipeline** for the Brazilian e‑commerce dataset (Olist).  
It includes:

- A **raw → processed** transformation pipeline  
- A **processed → analytics warehouse** loader  
- A **wipe/reset system** for safe re‑runs  
- A **full pipeline orchestrator**  
- Clean, color‑coded logging  
- Schema‑validated dimension and fact building  

The goal is to demonstrate a clean, maintainable, and reproducible data engineering workflow.

---

## 🏗️ Project Architecture

payflow_case_study/
│
├── python/
│   ├── transform.py              # Raw → processed ETL
│   ├── load_analytics_data.py    # Processed → analytics warehouse loader
│   ├── wipe_data.py              # Reset raw/processed/analytics layers
│   ├── run_all.py                # Full pipeline orchestration
│   └── old_processed_loader.py   # Deprecated (kept for reference)
│
├── data_base/
│   ├── raw_data/                 # Original Olist CSVs
│   └── processed_data/           # Cleaned & transformed datasets
│
├── sql/
│   └── analytics_schema.sql      # DDL for analytics schema
│
├── .env                          # Contains DB_URL
└── README.md

Code

---

## 🚀 Pipeline Overview

The pipeline runs in **two major stages**:

### **1. Raw → Processed (transform.py)**  
This stage:

- Normalizes column names  
- Renames fields to match analytics DDL  
- Cleans customers, merchants, and transactions  
- Converts timestamps  
- Saves processed datasets to `data_base/processed_data/`

### **2. Processed → Analytics Warehouse (load_analytics_data.py)**  
This stage:

- Loads processed datasets  
- Validates schema  
- Builds dimensions (customer, merchant, product, date, etc.)  
- Builds fact tables  
- Loads everything into the `analytics` schema in PostgreSQL  

---

## 🔄 Full Pipeline Execution

Run the entire pipeline with one command:

python python/run_all.py

Code

This performs:

1. **Wipe** raw, processed, and analytics schema  
2. **Transform** raw → processed  
3. **Load** processed → analytics warehouse  

All steps include clean, color‑coded logging.

---

## 🧹 Resetting the Environment

Use `wipe_data.py` to safely reset any layer:

python python/wipe_data.py processed
python python/wipe_data.py analytics
python python/wipe_data.py raw
python python/wipe_data.py all

Code

This ensures no stale files or mismatched schemas remain.

---

## 🧪 Processed Data Schema

### **Customers (clean_customers.csv)**

| Column                       | Description |
|------------------------------|-------------|
| customer_id                  | Unique customer ID |
| customer_unique_id           | Persistent customer identifier |
| customer_zip_code_prefix     | ZIP prefix |
| customer_city                | City |
| customer_state               | State |
| country                      | Always "Brazil" |

### **Merchants (clean_merchants.csv)**

| Column                       | Description |
|------------------------------|-------------|
| merchant_id                  | Seller ID |
| merchant_zip_code_prefix     | ZIP prefix |
| merchant_city                | City |
| merchant_state               | State |
| country                      | Always "Brazil" |

### **Transactions (transactions.csv)**  
Merged from orders, items, and payments.

---

## 🏛️ Analytics Schema

Defined in:

sql/analytics_schema.sql

Code

Created automatically during pipeline execution.

Includes:

- `dim_customer`
- `dim_merchant`
- `dim_product`
- `dim_date`
- `fact_orders`
- `fact_payments`
- `fact_items`
- etc.

---

## ⚙️ Environment Setup

### 1. Create virtual environment

python -m venv vvenv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows

Code

### 2. Install dependencies

pip install -r requirements.txt

Code

### 3. Configure `.env`

DB_URL=postgresql+psycopg2://user:password@localhost:5432/payflow

Code

---

## 🧭 Development Workflow

Typical workflow:

python python/wipe_data.py all
python python/run_all.py

Code

Or manually:

python python/transform.py
python python/load_analytics_data.py

Code

---

## 🧩 Notes & Design Decisions

- The old processed loader was deprecated to avoid schema conflicts  
- All analytics loaders now read exclusively from `processed_data/`  
- Schema validation prevents silent mismatches  
- DDL is executed before loading any analytics tables  
- Logging is consistent across all scripts  

---

## 📈 Future Enhancements

- Add dbt-style documentation  
- Add Airflow orchestration  
- Add unit tests for dimension builders  
- Add data quality checks (Great Expectations)  

---

## Author

**Yomi Ismail**

Data Engineering portfolio project focused on ETL design, PostgreSQL integration, and schema preparation for analytics use cases.

---
