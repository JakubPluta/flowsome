from __future__ import annotations
import enum
import os
from typing import Callable, List, Tuple
import enum
from typing import Any, List
import polars as pl
from flowsome.readers import PolarsFileReader
from flowsome.writers import PolarsFileWriter
from flowsome.log import get_logger
from flowsome.decorators import try_except

log = get_logger(__name__)


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


class TaskNode:
    """
    Node in a Directed Acyclic Graph (DAG)

    Each node has a task (str) and references to its children and parents
    """

    _type: TaskType

    def __init__(self, task_id: str, *args: Any, **kwargs: Any) -> None:
        """
        Initializes a TaskNode with the given task_id, args, and kwargs.

        :param task_id: The identifier of the task.
        :type task_id: str
        :param args: Positional arguments to be passed to the task.
        :type args: Any
        :param kwargs: Keyword arguments to be passed to the task.
        :type kwargs: Any
        :return: None
        """
        self.task_id: str = task_id
        self.children: List[TaskNode] = []
        self.parents: List[TaskNode] = []

        self._args = args
        self._params = kwargs

    def __repr__(self) -> str:
        return f"TaskNode(task_id={self.task_id})"

    def add_child(self, child: TaskNode) -> None:
        """
        Add a child Node to this node

        :param child: TaskNode - the child node to add
        :type child: TaskNode
        """
        self.children.append(child)

    def add_parent(self, parent: TaskNode) -> None:
        """
        Add a parent Node to this node

        :param parent: TaskNode - the parent node to add
        :type parent: TaskNode
        """
        self.parents.append(parent)

    def is_root(self) -> bool:
        """
        Check if the current node is a root node by verifying if it has no parents.

        :return: True if the current node is a root node, False otherwise
        :rtype: bool
        """
        return len(self.parents) == 0

    def is_leaf(self) -> bool:
        """
        Check if the current node is a leaf node by verifying if it has no children.

        :return: True if the current node is a leaf node, False otherwise
        :rtype: bool
        """
        return len(self.children) == 0

    def is_fan_in(self) -> bool:
        """
        Check if the current node is a fan-in node by verifying if it has more than one parent.

        :return: True if the current node is a fan-in node, False otherwise
        :rtype: bool
        """
        return len(self.parents) > 1

    def is_fan_out(self) -> bool:
        """
        Check if the current node is a fan-out node by verifying if it has more than one child.

        :return: True if the current node is a fan-out node, False otherwise
        :rtype: bool
        """
        return len(self.children) > 1


class ReadTask(TaskNode, PolarsFileReader):
    """Read data from a source in a specified format into a LazyFrame"""

    _type = TaskType.read

    def __init__(
        self, task_id: str, source: str | os.PathLike, *args: Any, **kwargs: Any
    ) -> None:
        """
        Initializes a ReadTask with the given task_id, source, args, and kwargs.

        :param task_id: The identifier of the task.
        :type task_id: str
        :param source: The source of the data to be read.
        :type source: str
        :param args: Positional arguments to be passed to the task.
        :type args: Any
        :param kwargs: Keyword arguments to be passed to the task.
        :type kwargs: Any
        :return: None
        """
        super().__init__(task_id, *args, **kwargs)
        self.source: os.PathLike | str = source

    @try_except
    def execute(self) -> pl.LazyFrame:
        """
        Executes the task by reading data from the specified source and returns a LazyFrame.

        :return: A LazyFrame containing the data read from the source.
        :rtype: pl.LazyFrame
        """
        return self.read(source=self.source, *self._args, **self._params)


class WriteTask(TaskNode, PolarsFileWriter):
    """Write LazyFrame to a destination in a specified format in a streaming mode"""

    _type = TaskType.write

    def __init__(
        self, task_id: str, file_path: os.PathLike | str, *args: Any, **params: Any
    ) -> None:
        """
        Initializes the WriteTask with the given task_id, file_path, args, and params.

        :param task_id: The identifier of the task.
        :type task_id: str
        :param file_path: The file path to write the data to.
        :type file_path: os.PathLike | str
        :param args: Positional arguments to be passed to the task.
        :type args: Any
        :param params: Keyword arguments to be passed to the task.
        :type params: Any
        :return: None
        """
        super().__init__(task_id, *args, **params)
        self.file_path: os.PathLike | str = file_path

    @try_except
    def execute(self, df: pl.LazyFrame) -> None:
        """
        Executes the task by writing the LazyFrame `df` to the file specified by `file_path` using the appropriate method.

        :param df: The LazyFrame to be written.
        :type df: pl.LazyFrame
        :return: None
        """
        return self.write(df, self.file_path, *self._args, **self._params)


class TransformTask(TaskNode):
    """Transform data in a LazyFrame"""

    _type = TaskType.transform

    def __init__(
        self, task_id: str, func: TransformMethods, *args: Any, **params: Any
    ) -> None:
        """
        Initializes the TransformTask with the given task_id, transform_method, args, and params.

        :param task_id: The identifier of the task.
        :type task_id: str
        :param func: The method to transform the LazyFrame.
        :type func: TransformMethods
        :param args: Positional arguments to be passed to the task.
        :type args: Any
        :param params: Keyword arguments to be passed to the task.
        :type params: Any
        :return: None
        """
        super().__init__(task_id, *args, **params)

        if func not in TransformMethods or not hasattr(pl.LazyFrame, func):
            raise ValueError(f"Transformation method {func} is not supported.")

        self.func: str = func

    @try_except
    def execute(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Executes the task by applying the transformation method to the LazyFrame `df`.

        :param df: The LazyFrame to be transformed.
        :type df: pl.LazyFrame
        :return: The transformed LazyFrame.
        :rtype: pl.LazyFrame
        """
        return getattr(df, self.func)(*self._args, **self._params)


class MergeTask(TaskNode):
    """Merge data from multiple sources into a single LazyFrame"""

    _type = TaskType.merge

    def _swap_dfs(
        self, df: pl.LazyFrame | pl.DataFrame, other_df: pl.LazyFrame | pl.DataFrame
    ) -> Tuple[pl.LazyFrame | pl.DataFrame, pl.LazyFrame | pl.DataFrame]:
        """
        A function that swaps the input dataframes based on the value of the 'how' parameter and returns the swapped dataframes as a tuple.
        :param df: The first DataFrame to be swapped
        :type df: pl.DataFrame | pl.LazyFrame
        :param other_df: The second DataFrame to be swapped
        :type other_df: pl.DataFrame | pl.LazyFrame
        :return: A tuple containing the swapped DataFrames
        :rtype: Tuple[pl.DataFrame | pl.LazyFrame, pl.DataFrame | pl.LazyFrame]
        """
        if self._params.get("how") == "right":
            self._params.update({"how": "left"})
            return other_df, df
        return df, other_df

    @try_except
    def execute(
        self, df: pl.DataFrame | pl.LazyFrame, other_df: pl.DataFrame | pl.LazyFrame
    ) -> pl.LazyFrame | pl.DataFrame:
        """
        Executes the task by merging the LazyFrames `df` and `other_df` using the join method.

        :param df: The first LazyFrame to be merged.
        :type df: pl.DataFrame | pl.LazyFrame
        :param other_df: The second LazyFrame to be merged.
        :type other_df: pl.DataFrame | pl.LazyFrame
        :return: The resulting merged LazyFrame.
        :rtype: pl.LazyFrame | pl.DataFrame
        """
        df, other_df = self._swap_dfs(df, other_df)
        return df.join(other_df, *self._args, **self._params)
