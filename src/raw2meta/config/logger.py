import logging
from datetime import datetime
from raw2meta.config.paths import  LOGS_DIR
from pathlib import Path
import os
import sys

logging_str = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"
DateFormat = "%Y-%m-%d %H:%M:%S"

log_dir = Path(LOGS_DIR).as_posix()
os.makedirs(log_dir, exist_ok=True) 

currentdate = datetime.now().strftime("%Y%m%d")
log_filepath = Path(LOGS_DIR / f"{currentdate}_message.log").as_posix()


def get_configured_logger(name: str = __name__) -> logging.Logger:
    """Get a configured logger instance.
    :param name: Name of the logger.
    :type name: str
    :return: Configured logger instance.
    :rtype: logging.Logger
    """
    logging.basicConfig(
        level=logging.INFO,
        format=logging_str,
        datefmt=DateFormat ,

        handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout)
    ]
    )
    return logging.getLogger(name)
