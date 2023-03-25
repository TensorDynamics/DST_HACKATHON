"""
Sets up the Logger to be implemented.
"""
import logging
from funcs import helpers


def setup_logger(name, log_file, level=logging.INFO):
    """
    Takes in the the name of logger to be created, the name of the log file to be created and
        the level of logging to be used ,i.e., INFO, DEBUG, ERROR etc.
    :param name: The name of the logger to be created.
    :type name: str
    :param log_file: The name of the log file to be created.
    :type log_file: str
    :param level: the level of logging to be used ,i.e., INFO, DEBUG, ERROR etc.
    :type level: str
    :return: logger class
    """
    formatter = logging.Formatter('%(levelname)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def start_loggers(parent_path, folder_name='LOGS'):
    """
    Start the loggers
    :param destination: The destination path for the logs to be saved
    :type destination: path like, str
    :return: logger to be used throughout the process
    :rtype: logger class
    """
    in_time = helpers.get_currenttime(string_or_date='date')
    helpers.create_folder(destination=parent_path, name=folder_name)

    logger_ = setup_logger(name='main_logger',
                           log_file='{destination}/Log_{stamp}.log'.format(destination=folder_name,
                                                                           stamp=in_time.strftime("%d-%b-%Y__%H_%M")))

    logger_.info('Start Time: {in_time}'.format(in_time=in_time))

    return logger_
