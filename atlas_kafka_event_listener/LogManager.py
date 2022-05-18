import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


class LogManager:
    DEFAULT_LOG_LEVEL = "DEBUG"  # better to have too much log than not enough

    def __init__(self, logger_name: str, enable_file_handler=False, log_propagate=False,
                 log_format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
                 log_file_path="~/atlas_client_log"):
        self.logger_name = logger_name
        self.__enable_file_handler = enable_file_handler
        self.__log_propagate = log_propagate
        self.__log_file_path = log_file_path
        self.__formatter = logging.Formatter(log_format)

    def get_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.__formatter)
        return console_handler

    def get_file_handler(self):
        Path(self.__log_file_path).mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(f"{self.__log_file_path}/app.log", when='midnight')
        file_handler.setFormatter(self.__formatter)
        return file_handler

    def get_logger(self):
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(os.environ.get("LOGLEVEL", LogManager.DEFAULT_LOG_LEVEL))
        logger.addHandler(self.get_console_handler())
        if self.__enable_file_handler:
            logger.addHandler(self.get_file_handler())
        # with this pattern, it's rarely necessary to propagate the error up to parent
        logger.propagate = self.__log_propagate
        return logger
