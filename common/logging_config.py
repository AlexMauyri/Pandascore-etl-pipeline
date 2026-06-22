import json
import logging.config
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

LOG_CONFIG_PATH = BASE_DIR / "configs" / "log_config.json"
LOG_PATH = BASE_DIR / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)

def setup_logging():
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    with open(LOG_CONFIG_PATH, encoding="utf-8") as config_file:
        config = json.load(config_file)
    
    today = datetime.now().strftime("%Y-%m-%d")
    log_filename = LOG_PATH / f"{today}.log"
    config["handlers"]["file"]["filename"] = str(log_filename)

    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info(f"Логирование настроено. Файл: {log_filename}")

