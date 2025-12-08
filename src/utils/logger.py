import logging
import os

def setup_logger(log_path="processamento.log"):
    # Garante que pasta existe
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("reddit_pipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # Log no arquivo
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    # Log no terminal
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    return logger
