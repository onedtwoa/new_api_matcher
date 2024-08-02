import logging
import os
from src.config import get_current_datetime
from logging.handlers import RotatingFileHandler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGGING_CONFIG = {
    "log_level": "INFO",
    "log_format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    "date_format": '%Y-%m-%d %H:%M:%S',
    "log_dir": os.path.join(BASE_DIR, 'logs'),
    "max_bytes": 1048576,
    "backup_count": 2,
    "log_to_console": True
}


def setup_logging(script_name):
    current_date_time = get_current_datetime()
    log_level = getattr(logging, LOGGING_CONFIG.get("log_level", "INFO").upper(), logging.INFO)

    logger = logging.getLogger(script_name)
    logger.setLevel(log_level)

    log_dir = os.path.join(LOGGING_CONFIG["log_dir"], script_name)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{script_name}_{current_date_time}.log')

    fh = RotatingFileHandler(log_file, maxBytes=LOGGING_CONFIG["max_bytes"], backupCount=LOGGING_CONFIG["backup_count"])
    fh.setLevel(log_level)

    if LOGGING_CONFIG["log_to_console"]:
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(logging.Formatter(LOGGING_CONFIG["log_format"], datefmt=LOGGING_CONFIG["date_format"]))
        logger.addHandler(ch)

    formatter = logging.Formatter(LOGGING_CONFIG["log_format"], datefmt=LOGGING_CONFIG["date_format"])
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger
