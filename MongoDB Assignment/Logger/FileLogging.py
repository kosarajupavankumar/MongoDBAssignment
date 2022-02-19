import logging
import os.path
from pathlib import Path


class FileLogger:
    def __init__(self):
        parent_path = Path(os.path.relpath('__file__')).parent
        os.makedirs(os.path.join(parent_path, 'log'), exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(parent_path, 'log', '.txt'),
            level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s'
        )

    def debug(self, messages):
        logging.DEBUG(messages)

    def info(self, messages):
        logging.info(messages)

    def error(self, messages):
        logging.error(messages)

    def warning(self, messages):
        logging.warning(messages)

    def critical(self, messages):
        logging.critical(messages)

    def disable(self):
        logging.disable()