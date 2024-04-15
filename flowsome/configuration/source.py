from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Sequence, Tuple

from polars import PolarsDataType


@dataclass
class BaseSourceParams:
    source: str | Path | List[str] | List[Path]
    cache: bool = True
    rechunk: bool = True
    n_rows: int | None = None
    row_index_name: str | None = None
    row_index_offset: int = 0


@dataclass
class CsvSourceParams(BaseSourceParams):
    has_header: bool = True
    separator: str = ","
    comment_prefix: str | None = None
    quote_char: str | None = '"'
    skip_rows: int = 0
    dtypes: Dict[str, PolarsDataType] | None = None
    schema: Dict[str, PolarsDataType] | None = None
    null_values: str | Sequence[str] | dict[str, str] | None = None
    missing_utf8_is_empty_string: bool = False
    ignore_errors: bool = False
    encoding: str = "utf8"
    low_memory: bool = False
    skip_rows_after_header: int = 0
    try_parse_dates: bool = False
    eol_char: Tuple[str] = ("\n",)
    new_columns: Sequence[str] | None = None
    raise_if_empty: bool = True
    truncate_ragged_lines: bool = False


@dataclass
class ParquetSourceParams(BaseSourceParams):
    parallel: Literal["auto", "columns", "row_groups", "none"] = "auto"
    use_statistics: bool = True
    hive_partitioning: bool = True
    storage_options: dict[str, Any] | None = None
    retries: int = 0


@dataclass
class JsonSourceParams(BaseSourceParams):
    schema: Dict[str, PolarsDataType] | None = None
    infer_schema_length: int | None = 100
    batch_size: int | None = 1024
    ignore_errors: bool = False


@dataclass
class IpcSourceParams(BaseSourceParams):
    storage_options: dict[str, Any] | None = None
    memory_map: bool = True
    retries: int = 0
