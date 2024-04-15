from __future__ import annotations

import os
from typing import Any, Callable, Dict, TypeAlias, Union
import polars as pl
from flowsome.log import get_logger


logger = get_logger(__name__)


PolarsSinkMethod: TypeAlias = Callable[..., None]
FileFormat: TypeAlias = str

LazyWriterMethods: Dict[FileFormat, PolarsSinkMethod] = {
    "csv": pl.LazyFrame.sink_csv,
    "ipc": pl.LazyFrame.sink_ipc,
    "parquet": pl.LazyFrame.sink_parquet,
    "json": pl.LazyFrame.sink_ndjson,
}


def register_lazy_writer(name: str | FileFormat, func: PolarsSinkMethod) -> None:
    """
    Registers a lazy writer method with a given name, and a function to be associated with it.

    :param name: The name of the lazy writer.
    :type name: str | FileFormat
    :param func: The function to be associated with the lazy writer.
    :type func: PolarsLazyWriteMethod
    :return: None
    """
    if name in LazyWriterMethods:
        raise ValueError(f"Writer with name {name} already registered")
    LazyWriterMethods[name] = func
    logger.info(f"Writer with name {name} registered")


class PolarsFileWriter:

    def _file_format(self, path: os.PathLike | str) -> FileFormat:
        """Get the file format from the file path.

        :param path: The path to the file.
        :type path: str, os.PathLike
        :return: The file format.
        :rtype: str
        """
        return os.path.splitext(path)[1][1:]

    def get_lazy_writer(self, fmt: str | FileFormat) -> PolarsSinkMethod:
        """
        Tries to return the lazy writer method associated with the given format.

        :param fmt: The format of the writer.
        :type fmt: str | FileFormat
        :return: The lazy writer method associated with the given format.
        :rtype: PolarsSinkMethod
        :raises ValueError: If the writer with the given format is not registered.
        """
        try:
            return LazyWriterMethods[fmt]
        except KeyError:
            raise ValueError(f"Writer with format {fmt} not registered")

    def write(self, df: pl.LazyFrame, path: os.PathLike | str, *args, **params) -> Any:
        """
        Writes the LazyFrame `df` to the file specified by `path` using the appropriate lazy writer method.

        :param df: The LazyFrame to be written.
        :type df: pl.LazyFrame
        :param path: The path to the file.
        :type path: os.PathLike | str
        :param args: Additional positional arguments for writing.
        :param params: Additional keyword arguments for writing.
        :return: The result of writing the LazyFrame to the file.
        :rtype: Any
        """
        fmt = self._file_format(path)
        return self.get_lazy_writer(fmt)(df, path, *args, **params)


__all__ = ["PolarsFileWriter"]
