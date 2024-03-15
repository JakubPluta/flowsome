# example of Pipeline
import json
from pipeline import read_data, write_data
from polars.datatypes import Struct, List
import polars as pl

# In case of converting files which support nested structures e.g parquet, json, delta 
# and we want to convert it into csv - we need to convert complex types to strings

def _list_to_str(column: str):
    return pl.col(column).cast(pl.List(pl.Utf8)).list.join(", ").alias(column)

def _struct_to_json(column: str):
    return pl.col(column).struct.json_encode().alias(column)

def stringify(data: pl.DataFrame) -> pl.DataFrame:
    schema = dict(data.schema)
    arr_to_convert = [k for k,v in schema.items() if isinstance(v, List)]
    struct_to_convert = [k for k,v in schema.items() if isinstance(v, Struct)]
    data = data.with_columns(
        *[_list_to_str(column) for column in arr_to_convert],
        *[_struct_to_json(column) for column in struct_to_convert]
        
    )
    return data
    
    