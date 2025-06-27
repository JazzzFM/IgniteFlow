"""
    sUMMARY:
    Script that contain the set of abstractions created to logger any
    process executed within the Machine Learning Operation Lifecycle.
"""
# UTF-8 PYTHON-3.6

from typing import Dict, List, Union, NoReturn
from dataclasses import dataclass, field
from datetime import datetime
from os import getenv
import logging
"""
This class takes care of the logs of the script. It is the parent class of the rest to follow
"""

@dataclass
class LogHandler:
    format_log: str = '%(asctime)s: %(levelname)s : %(name)s : %(message)s'
    logger_name: str = field(init = False)
    logger: object = field(init = False)
    log_file: str = field(init = False)
    handler: object = field(init = False)

    def __post_init__(self: object):
        self.log_file = getenv("LOG_FILE")
        now = datetime.now().strftime("%a %m %y")
        self.logger_name = getenv("MODEL_NAME") + "_" +\
                           getenv("BOX_NAME") + "_" +\
                           getenv("JOB_NAME") + "_" +\
                           now

        self.logger = logging.getLogger(f'{self.logger_name}')
        formatter = logging.Formatter(self.format_log)
        self.handler = logging.FileHandler(self.log_file)

        if self.logger.handlers:
            self.logger.handlers[0].close()
            self.logger.handlers = []

            self.handler.setLevel(logging.DEBUG)
            self.handler.setFormatter(formatter)

        self.handler.setLevel(logging.INFO)
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)

    @staticmethod
    def logger_dec(function):

        def logger_wrapper(self: object, *args, **kwargs):
            self.logger.setLevel(logging.INFO)
            self.log_info(f'Running: {function.__name__}')
            print(f'\n# -------------- Running: {function.__name__} -------------- #\n')
            try:
                func_return = function(self, *args, **kwargs)
                self.log_info(f'Done: {function.__name__}')
                print(f'\n# -------------- Finishing: {function.__name__} -------------- #\n')
                return func_return
            except Exception as e:

                self.log_info(f'Error while running: {function.__name__}')
                self.logger.exception(f'Error while running: {function.__name__}')
                print(f'\n# -------------- Error while running: {function.__name__} -------------- #\n')
                raise e
        return logger_wrapper

    def to_log(self: object, msg: str):
        sourceFile = open(self.log_file, 'w')
        print(msg, file = sourceFile)
        sourceFile.close()

    def log_info(self: object, msg = None):
        """
            Writes an info level log message into the logger used
        """
        if not msg:
            msg = self._get_msg_info()
        print("\n", msg, "\n")
        self.logger.info(msg)

    def log_debug(self, msg):
        """Writes a debug level log message into the logger used
        """
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(msg)

    def log_warning(self, msg):
        """Writes a warning level log message into the logger used
        """
        self.logger.setLevel(logging.WARNING)
        self.logger.warning(msg)

    def log_error(self, msg=None):
        """Writes an error level log message into the logger used
        """
        self.logger.setLevel(logging.ERROR)
        if not msg:
            msg = self._get_msg_error()
        self.logger.error(msg)

    def log_critical(self, msg):
        """Writes a critical level log message into the logger used
        """
        self.logger.setLevel(logging.CRITICAL)
        self.logger.critical(msg)

    def _get_msg_error(self):
        "Gets the standard error message to be logged."""
        msg = f"""
            Exception raised in {self.__class__.__name__}
        """
        return msg

    def _get_msg_info(self):
        "Gets the standard info message to be logged."""
        msg = f"""
            {self.__class__.__name__} executed correctly
        """
        return msg

