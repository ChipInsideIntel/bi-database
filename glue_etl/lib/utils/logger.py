# lib/utils/logger.py
import sys
import logging

class Logger:
    def __init__(self):
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger("glue_etl")
        self.logger.setLevel(logging.INFO)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)
