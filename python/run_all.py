import logging
import subprocess
import sys
from pathlib import Path

# ---------------------------------------
# Logging Configuration
# ---------------------------------------


class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[94m",     # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",    # Red
        "SUCCESS": "\033[92m",  # Green
        "RESET": "\033[0m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        record.msg = f"{color}{record.msg}{reset}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])

# ---------------------------------------
# Paths
# ---------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable  # ensures venv python is used

# ---------------------------------------
# Helper
# ---------------------------------------


def run_step(title: str, command: str):
    logging.info("\n" + "=" * 70)
    logging.info(f"🔷 {title}")
    logging.info("=" * 70 + "\n")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        logging.error(f"❌ {title} FAILED")
        sys.exit(result.returncode)

    logging.info(f"✔️  {title} completed successfully\n")


# ---------------------------------------
# Main Orchestration
# ---------------------------------------
def main():
    logging.info(
        "\n🚀 Starting Full Pipeline Run (wipe → transform → analytics load)\n")

    # 1. Wipe everything
    run_step(
        "WIPING DATA (raw + processed + analytics schema + operationals schema)",
        f"{PYTHON} {PROJECT_ROOT}/python/wipe_data.py all"
    )

    # 2. Run load_raw_data
    run_step(
        "LOADING RAW DATA",
        f"{PYTHON} {PROJECT_ROOT}/python/load_raw_data.py"
    )

    # 3. Run transform
    run_step(
        "RUNNING TRANSFORM PIPELINE",
        f"{PYTHON} {PROJECT_ROOT}/python/transform.py"
    )

    # 4. Run processed data loader
    run_step(
        "LOADING PROCESSED DATA",
        f"{PYTHON} {PROJECT_ROOT}/python/load_processed_data.py"
    )
    # 5. Run analytics loader
    run_step(
        "RUNNING ANALYTICS LOADER",
        f"{PYTHON} {PROJECT_ROOT}/python/load_analytics_data.py"
    )

    logging.info("\033[92m🎉 FULL PIPELINE COMPLETED SUCCESSFULLY!\033[0m\n")


# ---------------------------------------
# Entry Point
# ---------------------------------------
if __name__ == "__main__":
    main()
