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
        self._pipeline = []
        
    def read(self, file_path: str, schema: Dict | None = None, *args, **kwargs):
        func = getattr(pl, "scan_csv")
        self._pipeline.append(partial(func, file_path, schema=schema, *args, **kwargs))
        return self
    
    def filterby(self, *predicates, **kw_predicates):
        if not self._pipeline:
            raise ValueError("No read operation has been performed yet.")
        
        filter_func = getattr(pl.LazyFrame, "filter")
        self._pipeline.append(partial(filter_func, *predicates, **kw_predicates))
        return self
        

    def limit(self, n: int):
        if not self._pipeline:
            raise ValueError("No read operation has been performed yet.")
        limit_func = getattr(pl.LazyFrame, "limit")
        self._pipeline.append(partial(limit_func, n=n))
        return self
    
    def to_parquet(self, file_path: str, *args, **kwargs):
        backend = self._backend(self._pipeline)
        return backend.execute(file_path, *args, **kwargs)






        
        
    
    
            


