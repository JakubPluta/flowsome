from __future__ import annotations
import enum
from typing import Any
import polars as pl
from flowsome.mixins import ExecuteLazyFrameMethodMixin, ExecutePolarsMethodMixin
from flowsome.utils import get_scan_method, get_sink_method


class TaskType(str, enum.Enum):
    """Types of tasks that can be performed on the data"""
    read = "read"
    transform = "transform"
    write = "write"
    
    
class Operator:
    """Base class for operators"""
    _type: TaskType

    def __init__(self, name: str, *args, **params: Any) -> None:
        self._name = name
        self._args = args
        self._params = params

    def execute(self, *args, **kwargs) -> pl.LazyFrame | None:
        raise NotImplementedError


class ReadOperator(ExecutePolarsMethodMixin, Operator):
    """Read data from a source in a specified format into a LazyFrame"""
    
    _type = TaskType.read

    def __init__(self, name: str, fmt: str, *args,**params: Any) -> None:
        super().__init__(name, *args,**params)
        self._fn = get_scan_method(fmt)


class TransformOperator(ExecuteLazyFrameMethodMixin, Operator):
    """Transform data in a LazyFrame"""
    
    _type = TaskType.transform

    def __init__(self, name: str, fn: str, *args, **params: Any) -> None:
        super().__init__(name, *args, **params)
        self._fn = fn


class WriteOperator(ExecuteLazyFrameMethodMixin, Operator):
    """Write LazyFrame to a destination in a specified format in a streaming mode"""
    _type = TaskType.write

    def __init__(self, name: str, fmt: str, *args, **params: Any) -> None:
        super().__init__(name, *args,**params)
        self._fn = get_sink_method(fmt)
