from __future__ import annotations

import os
from typing import Any, Union
import polars as pl
from flowsome.log import get_logger



logger = get_logger(__name__)


PolarsLazyWriteMethod = Union[pl.LazyFrame.sink_csv, pl.LazyFrame.sink_parquet, pl.LazyFrame.sink_ndjson, pl.LazyFrame.sink_ipc]


class WritersRegistry:
    _instances = {}
    
    @classmethod
    def register_writer(cls, writer_instance):
        if writer_instance._fmt in cls._instances:
            raise ValueError(f"Writer with format {writer_instance._fmt} already registered")
        cls._instances[writer_instance._fmt] = writer_instance
    
    @classmethod
    def get_writer(cls, fmt):
        return cls._instances.get(fmt)
    

def get_writer(fmt: str) -> Writer:
    return WritersRegistry.get_writer(fmt)
    
class Writer:

    def __init__(self, writer: PolarsLazyWriteMethod, fmt: str) -> None:
        self._writer = writer
        self._fmt = fmt
        WritersRegistry.register_writer(self)
    
        
    def __repr__(self) -> str:
        return f"Writer(writer={self._writer.__name__}, fmt={self._fmt})"
    
    def write(self, df: pl.DataFrame, path: os.PathLike | str, *args, **params) -> Any:
        return self._writer(df, path, *args, **params)
    
    
CsvWriter = Writer(pl.LazyFrame.sink_csv, "csv")
IpcWriter = Writer(pl.LazyFrame.sink_ipc, "ipc")
ParquetWriter = Writer(pl.LazyFrame.sink_parquet, "parquet")
JsonWriter = Writer(pl.LazyFrame.sink_ndjson, "json")




class PolarsFileWriter:
    
    def write(self, df: pl.DataFrame, path: os.PathLike | str, *args, **params) -> Any:
        writer = get_writer(os.path.splitext(path)[1][1:])
        return writer.write(df, path, *args, **params)


__all__ = ["PolarsFileWriter"]