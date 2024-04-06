from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
from typing import Literal

@dataclass
class SinkCommonParams:
    """Parameters common to sinks"""
    path: str | Path
    maintain_order: bool = True
    type_coercion: bool = True
    predicate_pushdown: bool = True
    projection_pushdown: bool = True
    simplify_expression: bool = True
    slice_pushdown: bool = True
    no_optimization: bool = False


@dataclass
class SinkCsvParams(SinkCommonParams):
    """Parameters for sink_csv"""
    include_bom: bool = False
    include_header: bool = True
    separator: str = ","
    line_terminator: str = "\n"
    quote_char: str = '"'
    batch_size: int = 1024
    datetime_format: str | None = None
    date_format: str | None = None
    time_format: str | None = None
    float_precision: int | None = None
    null_value: str | None = None
    quote_style: Literal["necessary", "always", "numeric", "non_numeric"] | None = None


@dataclass
class SinkParquet(SinkCommonParams):
    compression: str = "zstd"
    compression_level: int | None = None
    statistics: bool = False
    row_group_size: int | None = None
    data_pagesize_limit: int | None = None


@dataclass
class SinkNdjson(SinkCommonParams):
    pass


@dataclass
class SinkIpc(SinkCommonParams):
    compression: str | None = "zstd"
