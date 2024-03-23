from flowsome import Client

class Dataset:
    pass


class DataReader:
    pass 


class Transformer:
    pass 
    

class Backend:
    
    def __init__(self, flow) -> None:
        self.flow = flow 
    def execute(self):
        pass
    
    
class PolarsBackend(Backend):
    def execute(self):
        print("executing polars flow")
        return self.execute(self.flow)
    


class Client:
    def __init__(self, backend: Backend, config):
        self._backend = backend
        self._config = config
        self._pipeline = []
        

    def read(self):
        """Interace for reading from polars"""
        pass 

    def filterby(self):
        pass 

    def limit(self, n=None):
        pass
    
    def to_parquet(self):
        return self._backend.execute(self)

