# PayFlow Database Optimization II

A data engineering case study that builds an ETL workflow around the Brazilian Olist e-commerce dataset. The project covers dataset acquisition, exploratory profiling, raw-to-processed transformations, and PostgreSQL loading into an operational schema.

## Project Overview

**Business context:** PayFlow is modeled as a fintech expanding into Brazil's e-commerce market. This repository focuses on preparing transaction-like commerce data for downstream analytics and operational reporting.

**Current scope:**
- Download the Olist dataset with the Kaggle CLI
- Explore source tables and profile data quality
- Clean customers and sellers data
- Merge orders, items, and payments into a raw transactions dataset
- Persist cleaned outputs to CSV/Parquet
- Load raw/cleaned tables into PostgreSQL under an `operationals` schema

## Repository Structure

```text
payflow_case_study/
├── data_base/
│   ├── raw_data/
│   └── processed_data/
├── python/
│   ├── download_data.py
│   ├── explore.py
│   ├── load_raw_data.py
│   └── transform.py
├── sql/
│   ├── create_raw_table.sql
│   └── setup_database.sql
├── NOTE.md
├── requirements.txt
└── .gitignore
```

## Tech Stack

- Python
- Pandas
- SQLAlchemy
- psycopg2
- python-dotenv
- PostgreSQL
- Kaggle CLI

## Data Flow

1. **Download** the Brazilian e-commerce dataset using the Kaggle CLI.
2. **Extract** the ZIP archive into `data_base/raw_data/`.
3. **Explore** source CSVs to understand shapes, columns, dtypes, and null counts.
4. **Transform** raw files into cleaned customer, merchant, and transaction datasets.
5. **Save** processed outputs to `data_base/processed_data/`.
6. **Load** tables into PostgreSQL in the `operationals` schema.

## Database Objects

### Schemas
- `operationals`
- `analytics` *(created, but not yet populated in the current implementation)*

### Tables currently defined
- `operationals.customers_raw`
- `operationals.merchants_raw`
- `operationals.transactions_raw`

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/yoismail/payflow_case_study.git
cd payflow_case_study
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it:

**Windows**
```bash
venv\Scripts\activate
```

**macOS / Linux**
```bash
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
DB_URL=postgresql+psycopg2://postgres:your_password@localhost:5432/payflow_db
```

### 5. Create the database and schemas

```bash
psql -U postgres -h localhost -f sql/setup_database.sql
psql -U postgres -h localhost -d payflow_db -f sql/create_raw_table.sql
```

### 6. Run the pipeline scripts

Download and extract data:

```bash
python python/download_data.py
```

Explore source data:

```bash
python python/explore.py
```

Transform and load data:

```bash
python python/load_raw_data.py
```

Optional transformation-only run:

```bash
python python/transform.py
```

## Key Transformation Logic

### Customers
- Deduplicate on `customer_id`
- Rename fields into cleaner operational names
- Add a constant `country = 'Brazil'`

### Sellers / Merchants
- Deduplicate on `seller_id`
- Rename `seller_id` to `merchant_id`
- Add a constant `country = 'Brazil'`

### Transactions
- Start from deduplicated `orders`
- Join `order_items` on `order_id`
- Join `order_payments` on `order_id`
- Convert delivery and approval timestamps to datetime

## Current Strengths

- Clear separation between Python and SQL assets
- Practical use of Pandas + PostgreSQL for ETL
- Good introduction to modular pipeline thinking
- Basic logging and exception handling are present
- Raw and processed storage layers are separated
- Secrets are excluded from version control through `.gitignore`

## Current Limitations

- No root `README.md` was present before this one, which weakens first impressions
- The implementation currently targets **operational/raw loading**, not a finished analytics layer
- No orchestration script ties the full workflow together end-to-end
- No tests, data quality assertions, or validation suite yet
- `if_exists="replace"` in `to_sql()` is risky for repeatable production-style loads
- Some scripts duplicate logic that should be centralized into reusable modules
- The `analytics` schema is created but not yet modeled into dimensional or reporting tables
- No CI/CD, release process, or automated linting is configured

## Recommended Next Steps

To make this repository portfolio-ready at a stronger mid-level to senior level:

1. Add a **single entry-point runner** for the full ETL flow.
2. Refactor shared logic into reusable modules (`extract`, `transform`, `load`, `config`, `utils`).
3. Add **data quality checks** for duplicates, nulls, row-count drift, and schema validation.
4. Introduce **analytics-ready modeling** such as fact and dimension tables.
5. Replace destructive `replace` loads with **append/upsert** strategies.
6. Add **logging to file** and structured execution summaries.
7. Add **unit tests** for transformation functions.
8. Add **SQL validation queries** and sample analytical questions.
9. Include **architecture diagrams** and a proper project narrative in the repo.
10. Add GitHub Actions for linting and test execution.

## Future Enhancements

- Star schema for orders, customers, sellers, products, and payments
- Incremental loading strategy
- Data warehouse schema under `analytics`
- Partitioning/indexing strategy for large transaction tables
- Pipeline orchestration with Airflow or Prefect
- Containerization with Docker
- Great Expectations or Pandera for quality controls

## Author

**Yomi Ismail**

Data Engineering portfolio project focused on ETL design, PostgreSQL integration, and schema preparation for analytics use cases.
