
---

# 🚀 PAYFLOW CASE STUDY — End‑to‑End Data Engineering Pipeline  
*A production‑grade ETL + Data Warehouse project I designed and built using Python, SQL, and PostgreSQL.*

---
 
## 🏷️ Badges  
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue)
![ETL Pipeline](https://img.shields.io/badge/ETL-Production--Grade-green)
![Logging](https://img.shields.io/badge/Logging-Cross--Platform-orange)
![Kaggle Dataset](https://img.shields.io/badge/Data-Olist%20E--Commerce-yellow)


---

# 🌟 Why This Project Matters  

I built this project to demonstrate how I approach **real‑world data engineering problems**—not just writing scripts, but designing systems.

My goals were clear:

- Build a **reliable** pipeline that never leaves the warehouse in an inconsistent state  
- Make it **reproducible**, so every run produces the same results  
- Ensure it’s **observable**, with structured logs and execution timings  
- Architect it to be **scalable**, with clean separation between layers  
- Deliver a **warehouse‑ready** star schema optimised for analytics  

This project reflects how I think as a data engineer:  
**structured, intentional, and focused on long‑term maintainability.**

---

# 🧠 What I Demonstrate in This Project

### 🔹 End‑to‑End ETL Engineering  
I designed and implemented a full pipeline from raw ingestion to analytics‑ready warehouse tables.

### 🔹 Real Data Modeling  
I built a complete star schema with facts, dimensions, surrogate keys, and referential integrity.

### 🔹 Production‑Ready Logging  
I engineered a cross‑platform logging system with color output, rotating file logs, and emoji‑aware formatting.

### 🔹 Clean Architecture  
I separated extract, explore, clean, transform, and orchestration into modular, testable components.

### 🔹 SQL + Python Integration  
I used SQLAlchemy for loading, raw SQL for schema creation, and pandas for transformation.

### 🔹 Idempotent Execution  
I ensured the pipeline can be run repeatedly with deterministic results.

---

# 🌐 High‑Level Architecture Diagram  

```
                ┌──────────────────────┐
                │      Kaggle API      │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │      RAW LAYER       │
                │  (Immutable CSVs)    │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │    STAGING LAYER     │
                │ (Cleaned 1:1 tables) │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │  TRANSFORM LAYER     │
                │ (Star Schema Build)  │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │ ANALYTICS WAREHOUSE  │
                │ (Facts + Dimensions) │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │       BI TOOLS       │
                └──────────────────────┘
```

---

# 🧩 ERD Diagrams  

## 📘 **Staging Schema ERD**

```
+---------------------------+
|   staging_customers       |
+---------------------------+
| customer_id (PK)          |
| customer_unique_id        |
| customer_zip_code_prefix  |
| customer_city             |
| customer_state            |
+---------------------------+

+---------------------------+
|   staging_orders          |
+---------------------------+
| order_id (PK)             |
| customer_id (FK)          |
| order_status              |
| order_purchase_timestamp  |
| order_approved_at         |
| order_delivered_carrier   |
| order_delivered_customer  |
| order_estimated_delivery  |
+---------------------------+

+---------------------------+
|   staging_order_items     |
+---------------------------+
| order_id (FK)             |
| order_item_id             |
| product_id                |
| seller_id                 |
| shipping_limit_date       |
| price                     |
| freight_value             |
+---------------------------+

+---------------------------+
|   staging_payments        |
+---------------------------+
| order_id (FK)             |
| payment_sequential        |
| payment_type              |
| payment_installments      |
| payment_value             |
+---------------------------+
```

---

## 📙 **Analytics (Star Schema) ERD**

```
                 +----------------------+
                 |     dim_customer     |
                 +----------------------+
                 | customer_key (PK)    |
                 | customer_id          |
                 | city                 |
                 | state                |
                 +----------▲-----------+
                            |
                            |
+---------------------------+---------------------------+
|                        fact_orders                   |
+------------------------------------------------------+
| order_key (PK)                                        |
| customer_key (FK)                                     |
| date_key (FK)                                         |
| order_status                                          |
| total_items                                           |
| total_payments                                        |
+---------------------------+---------------------------+
                            |
                            ▼
                 +----------------------+
                 |      dim_date        |
                 +----------------------+
                 | date_key (PK)        |
                 | day                  |
                 | month                |
                 | year                 |
                 | weekday              |
                 +----------------------+

+----------------------+      +----------------------+
|   dim_product        |      |   dim_seller         |
+----------------------+      +----------------------+
| product_key (PK)     |      | seller_key (PK)      |
| product_id           |      | seller_id            |
| category             |      | city                 |
| weight, size, etc.   |      | state                |
+----------▲-----------+      +----------▲-----------+
           |                                 |
           |                                 |
           +------------- fact_order_items ---+
```

---

# 🗂 Project Structure 

```
etl/
│── extract.py        # Download + validate raw data
│── explore.py        # Automated dataset exploration
│── clean.py          # Cleaning + staging load
│── transform.py      # Star schema builder
│── run_all.py        # Full pipeline orchestrator
│── wipe_all.py       # Environment reset tool
│── logger.py         # Cross‑platform logging system
│── db_config.py      # DB connection loader

data_base/
│── raw_data/         # Downloaded Kaggle data
│── cleaned_data/     # Cleaned CSV outputs

sql/
│── create_staging_tables.sql
│── create_analytics_tables.sql
│── setup_database.sql
```

---

# 🔄 Pipeline Flow 

### 1️⃣ Wipe  
I reset schemas and folders to guarantee a clean, deterministic run.

### 2️⃣ Extract  
I download, unzip, validate, and log metadata for all raw files.

### 3️⃣ Explore  
I automatically profile each dataset (shape, dtypes, missing values).

### 4️⃣ Clean  
I normalize, clean, and load data into the staging schema.

### 5️⃣ Transform  
I build the star schema (facts + dimensions) with surrogate keys.

### 6️⃣ Orchestrate  
I run the full DAG with structured logs and execution timings.

---

# 🧩 Advanced Logging System 

I built a custom logging system because production pipelines need more than `print()` statements.  
My logger includes:

- Color‑coded console logs  
- Smart emoji handling  
- Clean UTF‑8 rotating file logs  
- No ANSI codes in file logs  
- Timing decorators  
- Section banners  
- Deterministic handler setup  

This gives me full observability into every stage of the pipeline.

---

# ▶️ Running the Pipeline  

### Install dependencies  
```
pip install -r requirements.txt
```

### Configure environment  
```
DB_URL=postgresql://user:password@localhost:5432/payflow
```

### Run the full pipeline  
```
python -m etl.run_all
```

---

# 🤝 Contributing  

If you'd like to contribute, feel free to:

1. Fork the repo  
2. Create a feature branch  
3. Commit changes  
4. Open a pull request  

---

# 📄 License  

This project is released under the **MIT License**.

---

# 👤 Author  

**Yomi Ismail**  
Data Engineer & Product Operations Specialist  

---
