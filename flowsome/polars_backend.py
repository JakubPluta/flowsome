from typing import Sequence
import polars as pl 
from log import get_logger
from collections import deque
logger = get_logger(__name__)

SCAN_METHODS = {
    'csv': pl.scan_csv, # source
    'parquet': pl.scan_parquet, # source
    'json': pl.scan_ndjson, # source
    'delta': pl.scan_delta,
    'ipc': pl.scan_ipc,
    'iceberg': pl.scan_iceberg,
    'arrow': pl.scan_pyarrow_dataset,
    
}

SINK_METHODS = {
    "csv": pl.LazyFrame.sink_csv,
    "parquet": pl.LazyFrame.sink_parquet,
    "json": pl.LazyFrame.sink_ndjson,
    "ipc": pl.LazyFrame.sink_ipc,
}


class Config:
    """Contains the configuration of the pipeline 
    e.g. environment variables, connections, mode etc""" 



class Pipeline(deque):
    """Pipeline class that contains operations to be executed on the data"""


class Operation:
    pass 

class ReadOperation(Operation):
    def __init__(self, name: str, fmt: str, params: dict) -> None:       
        if fmt.lower() not in SCAN_METHODS:
            raise ValueError(f"Unsupported file format: {fmt}")
        self._name: str = name
        self._fn: callable = SCAN_METHODS[fmt]
        self._params: dict = params
        
    def __call__(self) -> pl.LazyFrame:
        return self._fn(**self._params)


class TransformOperation(Operation):
    def __init__(self, name: str, fn: str, params: dict) -> None:
        self._name = name
        self._fn = fn
        self._params = params
    
    def __call__(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            return getattr(df, self._fn)(**self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn)
            raise AttributeError from e 


class WriteOperation(Operation):
    def __init__(self, name: str, fmt: str, params: dict) -> None:
        if fmt.lower() not in SINK_METHODS:
            raise ValueError(f"Unsupported file format: {fmt}")
        self._name = name
        self._fn: callable = SINK_METHODS[fmt]
        self._params = params
    
    def __call__(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            return getattr(df, self._fn.__name__)(**self._params)
        except AttributeError as e:
            logger.error("Function %s not found", self._fn.__name__)
            raise AttributeError from e
        
  

def execute(pipeline: Pipeline | Sequence) -> pl.LazyFrame:
    steps = iter(pipeline)
    read_step = next(steps)
    write_step = None
    if not isinstance(read_step, ReadOperation):
        raise ValueError("First operation must be a ReadOperation")
    df: pl.LazyFrame = read_step()
    for step in steps:
        if isinstance(step, TransformOperation):
            df = step(df)
        elif isinstance(step, WriteOperation):
            write_step = step

    if write_step is None:
        return df
    return write_step(df)
  


def configure_pipeline(*operations: Operation):
    if not all(isinstance(op, Operation) for op in operations):
        raise ValueError("All operations must be of type Operation")
    pipeline = Pipeline()
    for op in operations:
        pipeline.append(op)

    return pipeline
    


read_op = ReadOperation("test", "csv", {"source": r"tests\data\sample.csv"})
t_filter = TransformOperation("test", "filter", {"Country": "Cyprus"})
t_limit = TransformOperation("test", "limit", {"n": 10})
write_op = WriteOperation("test", "csv", {"path": r"tests\data\output.csv"})

pipeline = configure_pipeline(read_op, t_filter, t_limit, write_op)
execute(pipeline)


    