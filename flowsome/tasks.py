from __future__ import annotations
import enum
from functools import wraps
import os
from typing import List
import enum
from typing import Any, List
import polars as pl
from flowsome.readers import PolarsFileReader
from flowsome.writers import PolarsFileWriter
from flowsome.log import get_logger
from flowsome.writers import PolarsFileWriter

log = get_logger(__name__)


class InvalidPipelineError(Exception):
    pass

class TaskExecutionError(Exception):
    pass


class TaskType(str, enum.Enum):
    """Types of tasks that can be performed on the data"""
    read = "read"
    transform = "transform"
    write = "write"
    merge = "merge"


class TransformMethods(str, enum.Enum):
    """Transform methods that can be applied to a LazyFrame"""
    filter = "filter"
    join = "join"
    select = "select"
    sort = "sort"
    limit = "limit"


def try_except(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            log.error("Error executing task with args %s and kwargs %s", args, kwargs)
            raise TaskExecutionError(str(e)) from e
    return wrapper



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

    
class ReadTask(TaskNode, PolarsFileReader):
    """Read data from a source in a specified format into a LazyFrame"""
    
    _type = TaskType.read
    
    def __init__(self, task_id: str, source: str, *args, **kwargs):
        super().__init__(task_id, *args, **kwargs)
        self.source = source
 
    @try_except
    def execute(self) -> pl.LazyFrame:
        return self.read(source=self.source, *self._args, **self._params)




class TransformTask(TaskNode):
    """Transform data in a LazyFrame"""
    
    _type = TaskType.transform

    def __init__(self, task_id: str, transform_method: TransformMethods, *args, **params: Any) -> None:
        super().__init__(task_id, *args, **params)
        self._func = transform_method
    
    @try_except
    def execute(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return getattr(df, self._func)(*self._args, **self._params)


class WriteTask(TaskNode, PolarsFileWriter):
    """Write LazyFrame to a destination in a specified format in a streaming mode"""
    _type = TaskType.write

    def __init__(self, task_id: str, file_path: os.PathLike | str, *args, **params: Any) -> None:
        super().__init__(task_id, *args,**params)
        self.file_path = file_path
    
    @try_except
    def execute(self, df: pl.LazyFrame) -> None:
        return self.write(df, self.file_path, *self._args, **self._params)


class MergeTask(TaskNode):
    """Merge data from multiple sources into a single LazyFrame"""
    _type = TaskType.merge
    
    
    @try_except
    def execute(self, df: pl.DataFrame, other_df: pl.DataFrame) -> pl.LazyFrame:
        if self._params.get("how") == "right":
            self._params["how"] = "left"
            return other_df.join(df, *self._args, **self._params)
        return df.join(other_df, *self._args, **self._params)