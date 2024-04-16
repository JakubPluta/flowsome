import logging


def get_logger(name: str) -> logging.Logger:
    """
    A function that returns a logger object based on the provided name.

    :param name: A string representing the name of the logger.
    :type name: str
    :return: A logging.Logger object.
    :rtype: logging.Logger
    """
    return logging.getLogger(name)
