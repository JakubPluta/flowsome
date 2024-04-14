from __future__ import annotations

from functools import wraps
import os
from typing import Any, Callable, Union
import polars as pl
from flowsome.log import get_logger



logger = get_logger(__name__)


PolarsLazyReadMethod = Union[pl.scan_csv, pl.scan_parquet, pl.scan_ndjson, pl.scan_ipc]


class ReaderRegistry:
    _instances = {}
    
    @classmethod
    def register_reader(cls, reader_instance):
        if reader_instance._fmt in cls._instances:
            raise ValueError(f"Reader with format {reader_instance._fmt} already registered")
        cls._instances[reader_instance._fmt] = reader_instance
    
    @classmethod
    def get_reader(cls, fmt):
        return cls._instances.get(fmt)



def get_reader(fmt: str) -> Reader:
    return ReaderRegistry.get_reader(fmt)



class Reader:

    def __init__(self, reader: PolarsLazyReadMethod, fmt: str) -> None:
        self._reader = reader
        self._fmt = fmt
        ReaderRegistry.register_reader(self)
    
        
    def __repr__(self) -> str:
        return f"Reader(reader={self._reader.__name__}, fmt={self._fmt})"
    
    def __call__(self, source: os.PathLike | str, *args, **params) -> Any:
        return self._reader(source, *args, **params)
    
    def read(self, source: os.PathLike | str, *args, **params) -> Any:
        return self._reader(source, *args, **params)


CsvReader = Reader(pl.scan_csv, "csv")
IpcReader = Reader(pl.scan_ipc, "ipc")
ParquetReader = Reader(pl.scan_parquet, "parquet")
JsonReader = Reader(pl.scan_ndjson, "json")





def path_exists_decorator(func: Callable[[Any, os.PathLike | str, Any, Any], Any]) -> Callable[[Any, os.PathLike | str, Any, Any], Any]:
    """
    Decorator to check if the file path exists before executing the wrapped function.

    :param func: The function to decorate.
    :type func: function
    :return: The wrapped function that checks the existence of the file path.
    :rtype: function
    """
    @wraps(func)
    def wrapper(
        self: Any,
        source: os.PathLike | str,
        *args: Any,
        **params: Any
    ) -> Any:
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




class PolarsFileReader:
    """
    A class for reading data from a file in a specified format.
    """


    @path_exists_decorator
    def read(self, source: os.PathLike | str, *args, **params) -> Any:
        """
        Read data from a file in a specified format.

        :param source: The path to the file to read.
        :type source: str, os.PathLike
        :param args: Additional args to pass to the reader.
        :param params: Additional keyword args to pass to the reader.
        :return: The data read from the file.
        :rtype: Any
        :raises FileNotFoundError: If the file specified by `source` does not exist.
        """
        fmt = os.path.splitext(source)[1][1:]
        reader: Reader = get_reader(fmt)
        return reader.read(source=source, *args, **params)




__all__ = ["PolarsFileReader"]