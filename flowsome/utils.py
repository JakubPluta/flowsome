from __future__ import annotations
import enum
from typing import Any
import polars as pl
from flowsome.log import get_logger
from collections import OrderedDict, deque

logger = get_logger(__name__)


LAZY_SCAN_METHODS = {
    "csv": pl.scan_csv,
    "parquet": pl.scan_parquet,
    "json": pl.scan_ndjson,
    "ipc": pl.scan_ipc,
    
    # TODO: currently you can read, but you can't write to this format. 
    # "delta": pl.scan_delta,
    # "iceberg": pl.scan_iceberg,
    # "arrow": pl.scan_pyarrow_dataset,
}

LAZY_SINK_METHODS = {
    "csv": pl.LazyFrame.sink_csv,
    "parquet": pl.LazyFrame.sink_parquet,
    "json": pl.LazyFrame.sink_ndjson,
    "ipc": pl.LazyFrame.sink_ipc,
}


def get_scan_method(fmt: str) -> callable:
    """
    A function that returns a scan method based on the input format.

    Parameters:
        fmt (str): The format to check against the available lazy scan methods.

    Returns:
        callable: The lazy scan method corresponding to the input format.
    """
    if fmt.lower() not in LAZY_SCAN_METHODS:
        raise ValueError(f"Unsupported file format: {fmt}")
    return LAZY_SCAN_METHODS[fmt]


def get_sink_method(fmt: str) -> callable:
    """
    Returns the appropriate sink method based on the given file format.

    Parameters:
        fmt (str): The file format for which the sink method is requested.

    Returns:
        callable: The sink method corresponding to the given file format.

    Raises:
        ValueError: If the given file format is not supported.

    Example:
        >>> get_sink_method("csv")
        <function sink_csv at 0x12345678>
    """
    if fmt.lower() not in LAZY_SINK_METHODS:
        raise ValueError(f"Unsupported file format: {fmt}")
    return LAZY_SINK_METHODS[fmt]

