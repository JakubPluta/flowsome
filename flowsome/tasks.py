from __future__ import annotations
import enum
from typing import List
import enum
from typing import Any, List
import polars as pl
from flowsome.readers.filereader import PolarsFileReader
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



class TaskNode:
    """
    Node in a Directed Acyclic Graph (DAG)

    Each node has a task (str) and references to its children and parents
    """
    
    _type: TaskType
    
    def __init__(self, task_id: str, *args, **kwargs):
        self.task_id: str = task_id
        self.children: List[TaskNode] = []
        self.parents: List[TaskNode] = []
        
        self._args = args
        self._params = kwargs

    def __repr__(self) -> str:
        return f"TaskNode(task_id={self.task_id}, children={self.children}, parents={self.parents})"
    

    def add_child(self, child):
        """Add a child Node to this node"""
        self.children.append(child)

    def add_parent(self, parent):
        """Add a parent Node to this node"""
        self.parents.append(parent)

    def is_root(self):
        """
        Check if the current node is a root node by verifying if it has no parents.
        """
        return len(self.parents) == 0
    
    def is_leaf(self) -> bool:
        return len(self.children) == 0
    
    def is_fan_in(self) -> bool:
        return len(self.parents) > 1
    
    def is_fan_out(self) -> bool:
        return len(self.children) > 1

    
class ReadTask(TaskNode):
    """Read data from a source in a specified format into a LazyFrame"""
    
    _type = TaskType.read

    def __init__(self, task_id: str, *args, **params: Any) -> None:
        super().__init__(task_id, *args, **params)

    def execute(self) -> pl.LazyFrame:
        try:
            return PolarsFileReader.read(*self._args, **self._params)
        except Exception as e:
            log.error("Error executing read task: %s", e)
            raise InvalidPipelineError from e

class TransformTask(TaskNode):
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


class WriteTask(TaskNode):
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


class MergeTask(TaskNode):
    """Merge data from multiple sources into a single LazyFrame"""
    _type = TaskType.merge
    
    
    def execute(self, df: pl.DataFrame, other_df: pl.DataFrame) -> pl.LazyFrame:
        if self._params.get("how") == "right":
            self._params["how"] = "left"
            return other_df.join(df, *self._args, **self._params)
        return df.join(other_df, *self._args, **self._params)