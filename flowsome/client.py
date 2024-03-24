from functools import partial
from typing import Dict, Sequence
import polars as pl


class Backend:
    def __init__(self, flow):
        self._flow = flow
        
    def execute(self, *args, **kwargs):
        raise NotImplementedError
    

    
class PolarsBackend(Backend):
    
    
    def execute(self, *args, **kwargs):
        data = self._flow.pop(0)()
        for fn in self._flow:
            data = fn(data)
        data.sink_parquet(*args, **kwargs) 
    
 

class Client:
    
    def __init__(self, backend: Backend, config=None):
        self._backend = backend
        self._config = config
        self._pipe = None
        
    def read(self, file_path: str, schema: Dict | None = None, *args, **kwargs):
        self._pipe = pl.scan_csv(file_path, schema=schema, *args, **kwargs)
        return self 
    
    @property
    def _empty_flow(self) -> bool:
        return self._pipe is None
    
    def filterby(self, *predicates, **kw_predicates):
        if self._empty_flow:
            raise ValueError("No read operation has been performed yet.")
        self._pipe = self._pipe.filter(*predicates, **kw_predicates)
        return self
    
    def limit(self, n: int):
        if self._empty_flow:
            raise ValueError("No read operation has been performed yet.")
        self._pipe = self._pipe.limit(n)
        return self
    
    def to_parquet(self, file_path: str, *args, **kwargs):
        if self._empty_flow:
            raise ValueError("No read operation has been performed yet.")
        self._pipe.sink_parquet(file_path, *args, **kwargs)
        return self
        
        
    
    
            


