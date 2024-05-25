"""LOG related utility module"""
import logging
import time
from functools import wraps
from typing import Optional, Union, Tuple


def _get_log_func(log_func: Union[logging.Logger, None],
                  *args,
                  **kwargs) -> Tuple[logging.Logger, bool]:
    """
    Returns the object logger if it exists, otherwise creates and returns a
    logger. Also returns whether the logger is found in the object or not.
    """
    if log_func:
        return log_func, False
    if args and hasattr(args[0], "log"):
        if isinstance(args[0].log, logging.Logger):
            return args[0].log, True
    return LoggerMixin().log, False


def log_time(log_func=None):
    """
    Decorator to log starting and ending points with a timer

    :param log_func: optional logger function
    :type log_func: logging.Logger
    """

    def log_decorator(func=None):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            log_f, _ = _get_log_func(log_func, *args, **kwargs)
            name = func.__name__
            starting_log = f"Starting: {name} function..."
            log_f.info(starting_log)
            output = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 2)
            ending_log = f"{name} is completed in {elapsed_time} sec"
            log_f.info(
                ending_log,
                extra={"elapsed_time": elapsed_time, "function_name": name}
            )
            return output

        return wrapper

    return log_decorator


class LoggerMixin:
    """
    Mixin class for logging

    :param name: name of the logger. if no name is specified, generate a name
        from module and class names
    :type name: str
    """
    _log: Optional[logging.Logger] = None

    @property
    def log(self) -> logging.Logger:
        """
        Return a :class:`logging.Logger` object with specified config
        """
        if not self._log:
            self._log = self._create_logger()
        return self._log

    def _create_logger(self) -> logging.Logger:
        """
        Create a Logger instance

        :return: a logger instance
        :rtype: logging.Logger
        """
        name = ".".join([
            self.__module__,
            self.__class__.__name__,
        ])
        return logging.getLogger(name)
