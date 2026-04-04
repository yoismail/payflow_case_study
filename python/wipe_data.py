import shutil
import logging
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data_base" / "raw_data"
PROCESSED_DIR = PROJECT_ROOT / "data_base" / "processed_data"


def delete_folder(path: Path):
    if path.exists():
        shutil.rmtree(path)
        logging.info(f"🗑️ Deleted folder: {path}")
    else:
        logging.info(f"⚠️ Folder not found (skipped): {path}")


def drop_analytics_schema():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in .env")

    engine = create_engine(db_url)

    logging.info("Dropping analytics schema...")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analytics CASCADE;"))
        conn.execute(text("CREATE SCHEMA analytics;"))
    logging.info("🧹 Analytics schema reset successfully.")


def wipe(mode: str):
    mode = mode.lower()

    if mode in ("processed", "all"):
        delete_folder(PROCESSED_DIR)

    if mode in ("raw", "all"):
        delete_folder(RAW_DIR)

    if mode in ("analytics", "all"):
        drop_analytics_schema()

    logging.info(f"✨ Wipe completed for mode: {mode}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Wipe ETL data folders and/or analytics schema.")
    parser.add_argument(
        "mode",
        choices=["raw", "processed", "analytics", "all"],
        help="Choose what to wipe."
    )

    args = parser.parse_args()
    wipe(args.mode)
