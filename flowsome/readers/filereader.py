from __future__ import annotations

import os
from typing import Any
import polars as pl
from flowsome.log import get_logger
from flowsome.readers.base import Reader, get_reader

logger = get_logger(__name__)




class PolarsFileReader:
    """
    A class for reading data from a file in a specified format.
    """

    @classmethod
    def read(self, file_path: os.PathLike | str, *args, **params) -> Any:
        """
        Read data from a file in a specified format.

        :param file_path: The path to the file to read.
        :type file_path: str, os.PathLike
        :param args: Additional args to pass to the reader.
        :param params: Additional keyword args to pass to the reader.
        :return: The data read from the file.
        :rtype: Any
        :raises FileNotFoundError: If the file specified by `file_path` does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        reader: Reader = get_reader(os.path.splitext(file_path.lower())[1][1:])
        return reader.read(file_path, *args, **params)


    
