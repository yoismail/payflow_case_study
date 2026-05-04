import logging
import os
import time

# Color Formatter (Console)


class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[94m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "SUCCESS": "\033[92m",
        "RESET": "\033[0m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        record.msg = f"{color}{record.msg}{reset}"
        return super().format(record)


# Create logs folder
os.makedirs("logs", exist_ok=True)


# Create logger
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
logger.handlers.clear()   # avoid duplicate logs if reloaded


# Console Handler (colored)
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)


# File Handler (no colors)
file_handler = logging.FileHandler("logs/pipeline.log")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)


# Helper utilities
def section(title: str):
    logger.info("\n" + "=" * 50)
    logger.info(f"🔷 {title}")
    logger.info("=" * 31 + "\n")


def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"⏱️ Step completed in {elapsed:.2f}s\n")
        return result
    return wrapper
