from __future__ import annotations
import enum
from typing import List
import enum
from typing import Any, List
import polars as pl
from flowsome.utils import get_scan_method, get_sink_method
from flowsome.log import get_logger

log = get_logger(__name__)


class InvalidPipelineError(Exception):
    pass


class TaskType(str, enum.Enum):
    """Types of tasks that can be performed on the data"""
    read = "read"
    transform = "transform"
    write = "write"
    merge = "merge"

class Task:
    
    _type: TaskType

    def __init__(self, task_id: str, *args, **params):
        self._args = args
        self._params = params
        self.task_id = task_id
        self.successor: Task = None
        self.predecessors: List[Task] = []
        
    def __repr__(self) -> str:
        return f"Task(task_id={self.task_id}, type={self._type})"

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def add_successor(self, task: "Task"):
        self.successor = task
        task.predecessors.append(self)

    def is_root(self) -> bool:
        return not self.predecessors

    def is_leaf(self) -> bool:
        return self.successor is None
    
    def is_merge_task(self) -> bool:
        return self._type == TaskType.merge

    


class ReadTask(Task):
    """Read data from a source in a specified format into a LazyFrame"""
    
    _type = TaskType.read

    def __init__(self, task_id: str, fmt: str, *args, **params: Any) -> None:
        super().__init__(task_id, *args, **params)
        self._reader = get_scan_method(fmt)
        
    def execute(self) -> pl.LazyFrame:
        try:
            return self._reader(*self._args, **self._params)
        except Exception as e:
            log.error("Error executing read task: %s", e)
            raise InvalidPipelineError from e

class TransformTask(Task):
    """Transform data in a LazyFrame"""
    
    _type = TaskType.transform

    def __init__(self, task_id: str, transform_method: str | callable, *args, **params: Any) -> None:
        super().__init__(task_id, *args, **params)
        self._transform_method = transform_method
        
    def execute(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            if isinstance(self._transform_method, str):
                return getattr(df, self._transform_method)(*self._args, **self._params)
            return getattr(df, self._transform_method.__name__)(*self._args, **self._params)
        except AttributeError as e:
            log.error("Function %s not found", self._transform_method)
            raise AttributeError from e


class WriteTask(Task):
    """Write LazyFrame to a destination in a specified format in a streaming mode"""
    _type = TaskType.write

    def __init__(self, task_id: str, fmt: str, *args, **params: Any) -> None:
        super().__init__(task_id, *args,**params)
        self._writer = get_sink_method(fmt)
        
    def execute(self, df: pl.LazyFrame) -> None:
        try:
            self._writer(df, *self._args, **self._params)
        except Exception as e:
            log.error("Error executing write task: %s", e)
            raise InvalidPipelineError from e


class MergeTask(TransformTask):
    """Merge data from multiple sources into a single LazyFrame"""
    _type = TaskType.merge
    
    
    def execute(self, df: pl.DataFrame, other_df: pl.DataFrame) -> pl.LazyFrame:
        if self._params.get("how") == "right":
            self._params["how"] = "left"
            return other_df.join(df, *self._args, **self._params)
        return df.join(other_df, *self._args, **self._params)
