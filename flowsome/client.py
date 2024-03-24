from typing import Callable, List


class Dataset:
    pass


class DataReader:
    pass


class Transformer:
    pass


class Backend:

    def execute(self, client: "Client"):
        pass


class PolarsBackend(Backend):
    def execute(self):
        print("executing polars flow")
        return self.execute(self.flow)


class Client:
    def __init__(self, config, backend: Backend = PolarsBackend()):
        self._backend = backend
        self._config = config
        self._pipeline: List[Callable] = []

    def read(self, **kwargs):
        """Interace for reading from polars"""
        pass

    def filterby(self, **kwargs):
        pass

    def limit(self, n=None):
        pass

    def to_parquet(self, **kwargs):
        return self._backend.execute(self)
