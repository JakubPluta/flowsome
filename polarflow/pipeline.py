import os
import enum
from functools import partial
import polars as pl

# TODO: Implement. Add validation of file path, reading multifiles from directory, auto-detecting file type 
# and how to read it with polars

class FileFormats(str, enum.Enum):
    CSV = 'CSV'
    PARQUET = 'PARQUET'
    JSON = 'JSON'
    DELTA = 'DELTA'

    
def _resolve_polars_file_reader(file_path: os.PathLike) -> partial:
    """
    A function that resolves the Polars file reader based on the file extension of the given file path.

    Args:
        file_path (os.PathLike): The path of the file to be read.

    Returns:
        partial: A partial function with the reader function and the file path as the source.
    """
    file_extension = os.path.splitext(file_path)[1].lstrip(".")
    reader_function = getattr(pl, f"read_{file_extension.lower()}")
    return partial(reader_function, source=file_path)

def _resolve_polars_file_writer(df: pl.DataFrame, file_path: os.PathLike) -> partial:
    """
    A function that resolves the Polars file writer based on the file extension of the given file path.

    Args:
        file_path (os.PathLike): The path of the file to be written to.

    Returns:
        partial: A partial function with the writer function and the file path as the destination.
    """
    file_extension = os.path.splitext(file_path)[1].lstrip(".")
    writer_function = getattr(df, f"write_{file_extension.lower()}")
    return partial(writer_function, file_path)

def read_data(file_path: os.PathLike, *args, **kwargs):
    """
    A function that reads data from a specified file path using a custom file reader.
    
    Args:
        file_path (os.PathLike): The path to the file to be read.
        *args: Additional positional arguments to be passed to the file reader function.
        **kwargs: Additional keyword arguments to be passed to the file reader function.
        
    Returns:
        The result of calling the resolved file reader function with the provided arguments and keyword arguments.
    """
    return _resolve_polars_file_reader(file_path)(*args, **kwargs)


def write_data(data: pl.DataFrame, file_path: os.PathLike, *args, **kwargs):
    """
    A function that writes data to a specified file path using a custom file writer.
    
    Args:
        data (pl.DataFrame): The data to be written to the file.
        file_path (os.PathLike): The path to the file to be written to.
        *args: Additional positional arguments to be passed to the file writer function.
        **kwargs: Additional keyword arguments to be passed to the file writer function.
        
    Returns:
        The result of calling the resolved file writer function with the provided arguments and keyword arguments.
    """
    return _resolve_polars_file_writer(data, file_path)(*args, **kwargs)


class Pipeline:
    def __init__(self, *steps: callable) -> None:
        self.steps = steps
        
    def execute(self, *args, **kwargs):
        pass 
    
