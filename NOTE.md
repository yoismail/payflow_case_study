# Create a virtual environment
python -m venv venv
venv\Scripts\activate - to activate

# Comand to install requirements
pip install -r requirements.txt - from the terminal

# psql -U postgres -h localhost -f sql/setup_database.sql

# 📦 Brazilian E-Commerce Dataset Script — Line-by-Line Explanation

---

## 🔹 Initial Print Statement

```python
print("Downloading Brazillian E-Commerce Dataset...")
```
* Displays a message in the console to indicate the script has started.
* Purely informational (no effect on logic).

---

## 🔹 Dataset URL

```python
url = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
```

* Stores the download link in a variable called `url`.
* This is the Kaggle API endpoint for the dataset.
* ⚠️ Important: This URL requires authentication (Kaggle API key).

---

## 🔹 Define Output Folder

```python
output_path = Path("data")
```
* Creates a `Path` object pointing to a folder named `data`.
* This is where downloaded and extracted files will be stored.

---

## 🔹 Create Folder (if not exists)

```python
output_path.mkdir(exist_ok=True)
```

* Creates the `data` directory if it doesn’t already exist.
* `exist_ok=True` prevents errors if the folder already exists.

---

## 🔹 Define Zip File Path

```python
zip_path = output_path / "dataset.zip"
```

* Creates the full file path: `data/dataset.zip`.
* `/` is overloaded in `Path` to join paths cleanly.

---

## 🔹 Download Start Message

```python
print("Downloading (2-5 minutes)...")
```

* Notifies the user that download is starting.
* Useful for long-running operations.

---

## 🔹 Download File

```python
urllib.request.urlretrieve(url, zip_path)
```

* Downloads the file from `url` and saves it to `zip_path`.
* Uses Python’s built-in `urllib` module.
* ⚠️ Will fail with Kaggle unless API authentication is set up.

---

## 🔹 Download Complete Message

```python
print("Download Complete!")
```

* Confirms that the download step finished.

---

## 🔹 Extraction Start Message

```python
print("Extracting files...")
```

* Indicates the script is moving to extraction phase.

---

## 🔹 Open Zip File

```python
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
```

* Opens the `.zip` file in **read mode (`'r'`)**.
* `with` ensures the file is properly closed after use.

---

## 🔹 Extract Files

```python
zip_ref.extractall(output_path)
```

* Extracts all contents of the zip file into the `data` folder.

---

## 🔹 Extraction Complete Message

```python
print("Extraction Complete!")
```

* Confirms extraction finished successfully.

---

## 🔹 Verification Start

```python
print("Verifying files...")
```

* Indicates the script will now check if files loaded correctly.

---

## 🔹 Load Customers Dataset

```python
customers = pd.read_csv(output_path / "olist_customers_dataset.csv")
```

* Reads the customers CSV file into a pandas DataFrame.
* `pd.read_csv()` loads structured data for analysis.

---

## 🔹 Load Sellers Dataset

```python
sellers = pd.read_csv(output_path / "olist_sellers_dataset.csv")
```

* Loads seller data into a DataFrame.

---

## 🔹 Load Order Items Dataset

```python
items = pd.read_csv(output_path / "olist_order_items_dataset.csv")
```

* Loads order item details into a DataFrame.

---

## 🔹 Count Rows (Customers)

```python
print(f"Customers: {len(customers):,}")
```

* Prints number of rows in `customers`.
* `:,` formats numbers with commas (e.g., 100,000).

---

## 🔹 Count Rows (Sellers)

```python
print(f"Sellers: {len(sellers):,}")
```

* Displays total number of sellers.

---

## 🔹 Count Rows (Items)

```python
print(f"Items: {len(items):,}")
```

* Displays total number of order items.

---

## 🔹 Final Confirmation

```python
print("All Data Downloaded!")
```

* Final message confirming the pipeline completed successfully.

---

# ⚠️ Key Issues in This Code

1. **Kaggle Authentication Missing**

   * `urllib` cannot download from Kaggle without credentials.
   * Fix: Use Kaggle API (`kaggle datasets download`).

2. **Hardcoded File Names**

   * Assumes exact filenames exist after extraction.
   * Could break if dataset structure changes.

3. **No Error Handling**

   * If download or extraction fails, script crashes.

---

# ✅ What This Script Does (Pipeline View)

| Step | Action                    |
| ---- | ------------------------- |
| 1    | Prints start message      |
| 2    | Defines download URL      |
| 3    | Creates storage directory |
| 4    | Downloads dataset         |
| 5    | Extracts zip file         |
| 6    | Loads CSV files           |
| 7    | Verifies by counting rows |

---

# 🧠 Conceptual Takeaway

This is a basic **ETL (Extract → Load → Verify)** pipeline:

* **Extract** → Download + unzip
* **Load** → Read into pandas
* **Verify** → Count rows

---
-----------------------------------------------------------------------------------------------------------------------------

 🧠 DETAILED EXPLANATION

---

## 🔹 Imports

```python
import subprocess
```

* Allows Python to run system commands (used to call Kaggle CLI).

```python
from pathlib import Path
```

* Provides an object-oriented way to handle file paths.

```python
import zipfile
```

* Handles zip file extraction.

```python
import pandas as pd
```

* Used to load and analyze CSV data.

```python
import logging
```

* Replaces `print()` with structured logs.

```python
import sys
```

* Allows program termination using `sys.exit()`.

---

## 🔹 Logging Configuration

```python
logging.basicConfig(...)
```

* Sets global logging behavior:

  * `INFO` → shows normal operations
  * Format includes timestamp + log level

---

## 🔹 Path Definitions

```python
DATA_DIR = Path("data")
```

* Defines where all files will be stored.

```python
ZIP_FILE = DATA_DIR / "brazilian-ecommerce.zip"
```

* Full path to downloaded dataset.

---

## 🔹 Step 1: Create Directory

```python
if not DATA_DIR.exists():
```

* Checks if folder exists.

```python
DATA_DIR.mkdir()
```

* Creates directory.

👉 Prevents crashes when writing files.

---

## 🔹 Step 2: Download Dataset

```python
subprocess.run([...], check=True)
```

* Executes Kaggle CLI command:

  ```
  kaggle datasets download -d olistbr/brazilian-ecommerce -p data
  ```

```python
check=True
```

* Raises error if command fails.

---

### Error Handling

```python
except subprocess.CalledProcessError:
```

* Handles download failure.

```python
sys.exit(1)
```

* Stops execution immediately (fail fast principle).

---

## 🔹 Step 3: Extract Dataset

```python
if not ZIP_FILE.exists():
```

* Ensures file exists before extraction.

```python
with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
```

* Opens zip safely.

```python
zip_ref.extractall(DATA_DIR)
```

* Extracts all files.

---

### Error Handling

```python
except zipfile.BadZipFile:
```

* Handles corrupted zip file.

---

## 🔹 Step 4: Validate Data

```python
customers = pd.read_csv(...)
```

* Loads dataset into memory.

```python
len(customers)
```

* Counts number of rows.

---

### Why validation matters:

* Confirms dataset downloaded correctly
* Prevents downstream errors in analysis

---

### Error Handling

```python
except FileNotFoundError
```

* Handles missing dataset files.

```python
except Exception
```

* Catches any unexpected errors.

---

## 🔹 Pipeline Orchestration

```python
def run_pipeline():
```

* Controls execution order.

```python
create_data_dir()
download_dataset()
extract_dataset()
validate_data()
```

* Executes ETL stages sequentially.

---

## 🔹 Entry Point

```python
if __name__ == "__main__":
```

* Ensures script runs only when executed directly.

---

# 🔥 WHAT MAKES THIS “PRODUCTION-GRADE”

| Feature                | Why it matters                 |
| ---------------------- | ------------------------------ |
| Logging                | Tracks execution for debugging |
| Error handling         | Prevents silent failures       |
| Validation             | Ensures data integrity         |
| Modularity             | Easy to extend                 |
| Fail-fast (`sys.exit`) | Stops bad pipelines early      |

---

# 🚀 NEXT STEP (HIGHLY RECOMMENDED)

To make this **portfolio-level strong**, you can add:

* Data cleaning (handle nulls, types)
* Load into PostgreSQL
* Config file (`.env`)
* Scheduling (Airflow / cron)

---

# 🧠 FINAL TAKEAWAY

You’ve now built a proper:

### **ETL Pipeline**

* **Extract** → Download from Kaggle
* **Load** → Extract + read CSV
* **Validate** → Check row counts

This is exactly the kind of structure used in real data engineering workflows.

---

