
---

# 🚀 PAYFLOW CASE STUDY — End‑to‑End Data Engineering Pipeline  
*A production‑grade ETL + Data Warehouse project built with Python, SQL, and PostgreSQL*

---

## 📌 Overview  
This project implements a **fully automated, reproducible, production‑style data pipeline** for the Brazilian E‑Commerce Public Dataset (Olist).  
It demonstrates real data engineering skills across:

- **Raw data ingestion**  
- **Data cleaning & standardization**  
- **Staging schema modeling**  
- **Star schema warehouse design**  
- **Fact & dimension construction**  
- **Orchestration & observability**  
- **Idempotent environment resets**  

The pipeline is modular, testable, and mirrors real‑world enterprise ETL workflows.

---

## 🧱 Architecture Summary  
The pipeline follows a classic **multi‑layer warehouse architecture**:

```
Raw Data → Staging → Transform → Analytics Warehouse → BI Layer
```

### Layers  
- **Raw Layer**  
  - Stores downloaded Kaggle CSVs  
  - Immutable source of truth  

- **Staging Layer**  
  - Cleaned, standardized tables  
  - 1:1 with raw data but normalized  
  - Loaded via SQLAlchemy  

- **Analytics Layer (Star Schema)**  
  - Dimensions: Customer, Seller, Product, Payment Type, Date  
  - Facts: Orders, Order Items, Payments  
  - Surrogate keys, FKs, indexes  

- **Orchestration Layer**  
  - `run_all.py` executes the full DAG  
  - `wipe_all.py` resets schemas & folders  
  - Logging + timing decorators  

---

## 🗂 Project Structure  

```
PAYFLOW_CASE_STUDY/
│
├── data_base/
│   ├── raw_data/          # Downloaded Kaggle data
│   └── cleaned_data/      # Cleaned CSV outputs
│
├── etl/
│   ├── extract.py         # Download + extract + validate raw data
│   ├── explore.py         # Automated dataset exploration
│   ├── clean.py           # Cleaning + staging load
│   ├── transform.py       # Star schema builder
│   ├── run_all.py         # Full pipeline orchestrator
│   ├── wipe_all.py        # Environment reset tool
│   ├── logger.py          # Color logging + timing
│   └── db_config.py       # DB connection loader
│
├── sql/
│   ├── create_staging_tables.sql
│   ├── create_analytics_tables.sql
│   └── setup_database.sql
│
├── .env
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🔄 Pipeline Flow  

### **1. Wipe Phase**
Resets the environment to a clean state:

- Deletes raw + cleaned folders  
- Drops & recreates `staging` and `analytics` schemas  
- Ensures deterministic pipeline runs  

### **2. Extract Phase**
- Downloads dataset from Kaggle  
- Extracts ZIP  
- Validates all CSVs  
- Logs row counts & missing values  

### **3. Explore Phase**
- Auto-discovers CSVs  
- Logs:
  - shape  
  - head  
  - dtypes  
  - missing values  

### **4. Clean Phase**
- Cleans customers, sellers, transactions  
- Handles cancellations  
- Converts timestamps  
- Saves cleaned CSVs  
- Loads into **staging schema**  

### **5. Transform Phase**
Builds a full **star schema**:

#### Dimensions  
- `dim_customer`  
- `dim_merchant`  
- `dim_product`  
- `dim_payment_type`  
- `dim_date`  

#### Facts  
- `fact_orders`  
- `fact_order_items`  
- `fact_payments`  

Includes:

- surrogate keys  
- date key mapping  
- lifecycle status  
- item counts  
- payment sequences  
- referential integrity  

### **6. Orchestration**
`run_all.py` executes:

1. wipe_all  
2. extract  
3. clean  
4. transform  

All steps are timed, logged, and fail‑fast.

---

## 🧠 Key Engineering Concepts Demonstrated  

### **✔ Modular ETL Architecture**  
Each stage is isolated, testable, and reusable.

### **✔ Production‑style Logging**  
Color‑coded logs, section banners, and timing decorators.

### **✔ Schema‑Driven Warehouse Design**  
All tables defined explicitly in SQL, not implicitly in Python.

### **✔ Idempotent Pipeline Execution**  
`wipe_all.py` ensures clean, repeatable runs.

### **✔ Star Schema Modeling**  
Optimized for analytics and BI workloads.

### **✔ SQL + Python Integration**  
SQLAlchemy used for staging + analytics loads.

### **✔ Data Quality Awareness**  
Validation at extract, clean, and transform stages.

---

## 🛠 Tech Stack  

| Layer | Tools |
|-------|--------|
| Language | Python 3.x |
| Data Processing | pandas |
| Database | PostgreSQL |
| ORM / Loader | SQLAlchemy |
| Environment | dotenv |
| Logging | custom ColorFormatter |
| Orchestration | Python subprocess DAG |
| Source Data | Kaggle (Olist Brazilian E‑Commerce) |

---

## ▶️ Running the Pipeline  

### **1. Install dependencies**
```
pip install -r requirements.txt
```

### **2. Set your `.env`**
```
DB_URL=postgresql://user:password@localhost:5432/payflow
```

### **3. Run the full pipeline**
```
python -m etl.run_all
```

---

## 📊 Warehouse Schema (Star Model)

### **Dimensions**
- Customer  
- Merchant 
- Product  
- Payment Type  
- Date  

### **Facts**
- Orders  
- Order Items  
- Payments  

Each fact table links to dimensions via surrogate keys.

---

## Author

**Yomi Ismail**
Data Engineer & Product Operations Specialist

---
