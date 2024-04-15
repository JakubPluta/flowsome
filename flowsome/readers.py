from __future__ import annotations

from functools import wraps
import os
from typing import Any, Callable, Dict, TypeAlias, Union
import polars as pl
from flowsome.log import get_logger
from flowsome.decorators import path_exists


logger = get_logger(__name__)

PolarsScanMethod: TypeAlias = Callable[..., pl.LazyFrame]
FileFormat: TypeAlias = str

LazyReadersMethods: Dict[FileFormat, PolarsScanMethod] = {
    "csv": pl.scan_csv,
    "ipc": pl.scan_ipc,
    "parquet": pl.scan_parquet,
    "json": pl.scan_ndjson,
}


def register_lazy_reader(name: str | FileFormat, func: PolarsScanMethod) -> None:
    """
    Registers a lazy reader method with a given name, and a function to be associated with it.

    :param name: The name of the lazy reader.
    :type name: str | FileFormat
    :param func: The function to be associated with the lazy reader.
    :type func: PolarsScanMethod
    :return: None
    """
    if name in LazyReadersMethods:
        raise ValueError(f"Reader with name {name} already registered")
    LazyReadersMethods[name] = func
    logger.info(f"Reader with name {name} registered")


class PolarsFileReader:
    """
    A class for reading data from a file in a specified format.
    """

    def _file_format(self, source: os.PathLike | str) -> FileFormat:
        """Get the file format from the file path.

        :param source: The path to the file.
        :type source: str, os.PathLike
        :return: The file format.
        :rtype: str
        """
        return os.path.splitext(source)[1][1:]

    def get_lazy_reader(self, fmt: str | FileFormat) -> PolarsScanMethod:
        """
        Tries to return the lazy reader method associated with the given format.

        :param fmt: The format of the reader.
        :type fmt: str | FileFormat
        :return: The lazy reader method associated with the format.
        :rtype: PolarsScanMethod
        :raises ValueError: If the reader with the specified format is not registered.
        """
        try:
            return LazyReadersMethods[fmt]
        except KeyError:
            raise ValueError(f"Reader with format {fmt} not registered")

    @path_exists
    def read(self, source: os.PathLike | str, *args, **params) -> pl.LazyFrame:
        """
        Read data from a source in a specified format into a LazyFrame

        :param source: The path to the file.
        :type source: str, os.PathLike
        :param args: Additional args to pass to the lazy reader method.
        :param params: Additional keyword args to pass to the lazy reader method.
        :return: A LazyFrame
        :rtype: pl.LazyFrame

        """
        fmt = self._file_format(source)
        return self.get_lazy_reader(fmt)(source, *args, **params)


__all__ = ["PolarsFileReader"]
