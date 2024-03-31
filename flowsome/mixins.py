from __future__ import annotations
import polars as pl
from flowsome.log import get_logger


logger = get_logger(__name__)

class ExecuteLazyFrameMethodMixin:
    """
    Mixin class for executing a method on a `pl.LazyFrame` object.

    Attributes:
        _fn (Union[str, Callable]): The name of the method to execute or the method itself.
        _args (Tuple): Positional arguments to pass to the method.
        _params (Dict): Keyword arguments to pass to the method.

    Methods:
        execute(df: pl.LazyFrame) -> pl.LazyFrame: Executes the method on the `df` object.
    """

    def execute(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Executes the method on the `df` object.

        :param df: The `pl.LazyFrame` object on which to execute the method.
        :type df: pl.LazyFrame

        :return: The result of executing the method on the `df` object.
        :rtype: pl.LazyFrame

        :raises AttributeError: If the specified method is not found in the `pl.LazyFrame` object.
        """
        try:
            if isinstance(self._fn, str):
                return getattr(df, self._fn)(*self._args, **self._params)
            return getattr(df, self._fn.__name__)(*self._args, **self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn.__name__)
            raise AttributeError from e

class ExecutePolarsMethodMixin:
    """
    Mixin class for executing a method on a `pl.LazyFrame` object.

    Methods:
        execute() -> pl.LazyFrame: Executes the specified method on the `pl.LazyFrame` object.
    """

    def execute(self) -> pl.LazyFrame:
        """
        Executes the specified method on the `pl.LazyFrame` object.

        :return: The result of executing the specified method on the `pl.LazyFrame` object.
        :rtype: pl.LazyFrame

        :raises AttributeError: If the specified method is not found.
        """
        try:
            return self._fn(*self._args, **self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn.__name__)
            raise AttributeError from e