from __future__ import annotations
from typing import List
import polars as pl
from flowsome.log import get_logger
from flowsome import operators
from collections import OrderedDict, deque

logger = get_logger(__name__)


class LazyReaderConfig:
    pass 

class LazySinkConfig:
    pass 

class TransformConfig:
    pass 




# TODO: support multi readers -> downstream transformations needs to know source EG. reader1 -> t1,t2 reader2 -> t3
class Pipeline:
    """Pipeline class that contains operations to be executed on the data"""

    def __init__(self, *operations: operators.Operator) -> None:
        """
        Initializes a new instance of the Pipeline class with the given operations.

        :param *operations (operators.Operator): The operations to be added to the pipeline.
        
        :raises:
            - ValueError: If any of the operations are not of type operators.Operator.
            - ValueError: If the pipeline does not contain at least one read operation.
        
        :return: None
        """
        if not all(isinstance(op, operators.Operator) for op in operations):
            raise ValueError("All operations must be of type Operation")
        
        self.reader, self.transformers, self.writers = self._dispatch(*operations)
        if self.reader is None:
            raise ValueError("Pipeline must contain at least one read operation")

    def _dispatch(self, *operations: operators.Operator) -> tuple[operators.ReadOperator | None, List[operators.TransformOperator], List[operators.WriteOperator]]:
        """
        Dispatches the given operations to their respective task types: read, transform, and write.
        
        :param *operations (operators.Operator): The operations to be dispatched.
        :return: tuple[operators.ReadOperator | None, List[operators.TransformOperator], List[operators.WriteOperator]]:
                 A tuple containing the read operator, a list of transform operators, and a list of write operators.
                 The read operator is None if no read operation is provided.
        """
        operators_mapping = OrderedDict(
            {
                operators.TaskType.read: None,
                operators.TaskType.transform: deque(),
                operators.TaskType.write: [],
            }
        )
        op: operators.Operator
        for op in operations:
            if op._type == operators.TaskType.read:
                operators_mapping[op._type] = op
            else:
                operators_mapping[op._type].append(op)
        return operators_mapping.values()

    def run(self) -> pl.LazyFrame:
        """
        A method to run the entire workflow, including executing readers, transformers, and writers,
        and returning the resulting LazyFrame or writing it to a destination if writer is provided.
        """
        df: pl.LazyFrame = self.reader.execute()
        for t in self.transformers:
            df = t.execute(df)
        for w in self.writers:
            w.execute(df)
        return df
