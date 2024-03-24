from client import Client
from client import PolarsBackend
from configuration.config import Config
import polars as pl

client = Client(backend=PolarsBackend, config=Config())

file_path = r'tests\data\sample.csv'

client.read(file_path, schema=None).filterby(Country = "Malta").limit(n=100).to_parquet(file_path="data.parquet")

pq = pl.scan_parquet(r"data.parquet")
print(pq.collect())