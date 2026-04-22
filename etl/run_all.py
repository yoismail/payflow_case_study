import logging
import subprocess
import sys
from pathlib import Path


# Logging Configuration
from etl.logger import ColorFormatter, section, timed

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable  # ensures venv python is used

# Helper function to run steps with logging and error handling


def run_step(title: str, command: str):
    logging.info("\n" + "=" * 70)
    logging.info(f"🔷 {title}")
    logging.info("=" * 70 + "\n")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        logging.error(f"❌ {title} FAILED")
        sys.exit(result.returncode)

    logging.info(f"✔️  {title} completed successfully\n")


# Main Orchestration
@timed
def main():
    logging.info(
        "\n🚀 Starting Full Pipeline Run (wipe → extract → transform → analytics load)\n")

    # 1. Wipe everything
    run_step(
        "WIPING DATA",
        f"{PYTHON} -m etl.wipe_all all"
    )

    # 2. Run extract_raw_data
    run_step(
        "EXTRACTING RAW DATA",
        f"{PYTHON} -m etl.extract"

    )

    # 3. Run clean loader and loads cleaned data into staging schema
    run_step(
        "RUNNING CLEAN PIPELINE",
        f"{PYTHON} -m etl.clean"
    )

    # 4. Run analytics loader and loads data into analytics schema
    run_step(
        "RUNNING ANALYTICS LOADER",
        f"{PYTHON} -m etl.transform"
    )

    logging.info("\033[92m🎉 FULL PIPELINE COMPLETED SUCCESSFULLY!\033[0m\n")


# Entry Point
if __name__ == "__main__":
    main()
