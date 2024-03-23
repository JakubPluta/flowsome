from flowsome import Client


config = object

client = Client(backend='polars', config=config)

client.read(
    file="data.csv", 
    schema="infer", 
    columns=['col1', 'col2', 'col2']
).filter_by("col1 = abc").limit(n=100).to_parquet(
    file="data.parquet", partition_by=['col1'] 
)
