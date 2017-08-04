import os
import logging

from logging.handlers import RotatingFileHandler


def activate_logging(*, log_path=None, level=20):
    """Configure logging.

    Args:
        log_path (str): Optional full path for log file.
        level (int): Logging level, some options are
            40 - ERROR
            30 - WARNING
            20 - INFO
            10 - DEBUG
        config (dict): Configuration parameters of the project
    """
    # define logging output
    log_formatter = logging.Formatter("%(asctime)s - "
                                      "%(filename)s.%(funcName)s - "
                                      "%(levelname)s - %(message)s")
    # setup logging to the console
    handlers = [logging.StreamHandler()]

    # setup logging to a file
    if log_path:
        # make directory to store log file
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        # add extension
        log_path = log_path if log_path.endswith('.log') \
            else '.'.join([log_path, 'log'])
        FileHandler = RotatingFileHandler(
            log_path,
            maxBytes=200000000,
            backupCount=5
        )
        handlers.append(FileHandler)

    # add handlers to root logger
    RootLogger = logging.getLogger()
    RootLogger.setLevel(level)
    for h in handlers:
        h.setFormatter(log_formatter)
        RootLogger.addHandler(h)

    if log_path:
        logging.info("Log file for this session {}".format(log_path))
    logging.info("Logging activated with level {}".format(level))
