import logging
from .misc import get_time_export_string
import time
# from snowflake.connector.secret_detector import SecretDetector

def log(action, type, message):
    # Gets or creates a logger
    logger = logging.getLogger(action)

    if (logger.hasHandlers()):
        logger.handlers.clear()

    # set log level
    logger.setLevel(logging.DEBUG)

    # define file handler and set formatter
    filename = '%s.log' % (action + "_" + get_time_export_string())
    file_handler = logging.FileHandler('logs/'+filename, mode='a')
    # formatter_with_secret_detector = SecretDetector('%(asctime)s :: [%(levelname)s] :: [%(name)s] :: %(message)s')
    # file_handler.setFormatter(formatter_with_secret_detector)
    formatter = logging.Formatter('%(asctime)s :: [%(levelname)s] :: [%(name)s] :: %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
    logging.Formatter.converter = time.localtime
    file_handler.setFormatter(formatter)

    # add file handler to logger
    logger.addHandler(file_handler)

    if (type == "INFO"):
        return logger.info(message)
    elif (type == "DEBUG"):
        return logger.debug(message)
    elif (type == "WARNING"):
        return logger.warning('WARNING - ' + message)
    elif (type == "ERROR"):
        return logger.error('ERROR - ' + message)
    elif (type == "CRITICAL"):
        return logger.critical('CRITICAL ERROR - ' + message)
    else:
        return logger.debug(message)


# sample
# log("test_log", "DEBUG", "Test message")
# log("test_log", "INFO", "Some info here")