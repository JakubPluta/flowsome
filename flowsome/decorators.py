from __future__ import annotations
import os
from typing import Any, Callable
from functools import wraps
from flowsome.log import get_logger
from flowsome.exc import TaskExecutionError

logger = get_logger(__name__)


def try_except(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to wrap a function with try-except block, catching any exception
    and logging an error before raising a TaskExecutionError.

    :param func: The function to be wrapped with try-except block.
    :type func: Callable[[Any, Any, Any], Any]
    :return: A wrapped function with try-except block.
    :rtype: Callable[[Any, Any, Any], Any]
    :raises TaskExecutionError: If an exception is caught during
    the execution of the wrapped function.
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Wraps the function with try-except block, catching any exception
        and logging an error before raising a TaskExecutionError.

        :param self: The instance of the class that the wrapped function is a method of.
        :type self: Any
        :param args: The function args.
        :param kwargs: The function keyword args.
        :return: The return value of the wrapped function.
        :rtype: Any
        :raises TaskExecutionError: If an exception is caught during
        the execution of the wrapped function.
        """
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.error(
                "Error executing task with args %s and kwargs %s", args, kwargs
            )
            raise TaskExecutionError(str(e)) from e

    return wrapper


def path_exists(
    func: Callable[[Any, os.PathLike | str, Any, Any], Any]
) -> Callable[[Any, os.PathLike | str, Any, Any], Any]:
    """
    Decorator to check if the file path exists before executing the wrapped function.

    :param func: The function to decorate.
    :type func: function
    :return: The wrapped function that checks the existence of the file path.
    :rtype: function
    """

    @wraps(func)
    def wrapper(self: Any, source: os.PathLike | str, *args: Any, **params: Any) -> Any:
        """
        Check if the file path exists before executing the wrapped function.

        :param self: The instance of the class that the wrapped function is a method of.
        :type self: Any
        :param source: The file path to check.
        :type source: str, os.PathLike
        :param args: Additional args to pass to the wrapped function.
        :param params: Additional keyword args to pass to the wrapped function.
        :return: The return value of the wrapped function.
        """
        if not os.path.exists(source):
            raise FileNotFoundError(f"File not found: {source}")
        return func(self, source, *args, **params)

    return wrapper
