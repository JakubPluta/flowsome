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
    "delta": pl.scan_delta,
    "ipc": pl.scan_ipc,
    "iceberg": pl.scan_iceberg,
    "arrow": pl.scan_pyarrow_dataset,
}

LAZY_SINK_METHODS = {
    "csv": pl.LazyFrame.sink_csv,
    "parquet": pl.LazyFrame.sink_parquet,
    "json": pl.LazyFrame.sink_ndjson,
    "ipc": pl.LazyFrame.sink_ipc,
}


class OperationTypes(str, enum.Enum):
    read = "read"
    transform = "transform"
    write = "write"


class ExceuteDataFrameMethodMixin:

    def execute(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            if isinstance(self._fn, str):
                return getattr(df, self._fn)(**self._params)
            return getattr(df, self._fn.__name__)(**self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn.__name__)
            raise AttributeError from e


class ExecutePolarsMethodMixin:

    def execute(self) -> pl.LazyFrame:
        try:
            return self._fn(**self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn.__name__)
            raise AttributeError from e


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


class Config:
    """Contains the configuration of the pipeline
    e.g. environment variables, connections, mode etc"""


# TODO: support multi readers -> downstream transformations needs to know source
# EG. reader1 -> t1,t2
#     reader2 -> t3
class Pipeline:
    """Pipeline class that contains operations to be executed on the data"""

    def __init__(self, *operations: Operation) -> None:
        if not all(isinstance(op, Operation) for op in operations):
            raise ValueError("All operations must be of type Operation")
        self._operations = operations
        self.reader, self.transformers, self.writers = self._dispatch(*operations)
        if self.reader is None:
            raise ValueError("Pipeline must contain at least one read operation")

    def _dispatch(self, *operations: Operation) -> OperationTypes:
        op_map = OrderedDict(
            {
                OperationTypes.read: None,
                OperationTypes.transform: deque(),
                OperationTypes.write: [],
            }
        )
        op: Operation
        for op in operations:
            if op.op_type == OperationTypes.read:
                op_map[op.op_type] = op
            else:
                op_map[op.op_type].append(op)
        return op_map.values()

    def run(self) -> pl.LazyFrame:
        df: pl.LazyFrame = self.reader.execute()
        for t in self.transformers:
            df = t.execute(df)
        for w in self.writers:
            w.execute(df)
        return df


class Operation:
    op_type: OperationTypes

    def __init__(self, name: str, **params: Any) -> None:
        self._name = name
        self._params = params

    def execute(self, *args, **kwargs) -> pl.LazyFrame | None:
        raise NotImplementedError


class ReadOperation(ExecutePolarsMethodMixin, Operation):
    op_type = OperationTypes.read

    def __init__(self, name: str, fmt: str, **params: Any) -> None:
        super().__init__(name, **params)
        self._fn = get_scan_method(fmt)


class TransformOperation(ExceuteDataFrameMethodMixin, Operation):
    op_type = OperationTypes.transform

    def __init__(self, name: str, fn: str, **params: Any) -> None:
        super().__init__(name, **params)
        self._fn = fn


class WriteOperation(ExceuteDataFrameMethodMixin, Operation):
    op_type = OperationTypes.write

    def __init__(self, name: str, fmt: str, **params: Any) -> None:
        super().__init__(name, **params)
        self._fn = get_sink_method(fmt)
