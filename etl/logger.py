import logging
import time


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


handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


def section(title: str):
    logging.info("\n" + "=" * 50)
    logging.info(f"🔷 {title}")
    logging.info("=" * 31 + "\n")


def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logging.info(f"⏱️  Step completed in {elapsed:.2f}s\n")
        return result
    return wrapper
