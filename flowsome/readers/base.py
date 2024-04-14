from __future__ import annotations

import os
from typing import Any, Union
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




def get_reader(fmt: str) -> Reader:
    return ReaderRegistry.get_reader(fmt)


__all__ = ["CsvReader", "IpcReader", "ParquetReader", "JsonReader", "get_reader"]