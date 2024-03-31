from flowsome.pipeline import Pipeline
from flowsome.operators import ReadOperator, TransformOperator, WriteOperator


import polars as pl 

if __name__ == "__main__":
    
    read_op = ReadOperator("test", "csv", source= r"tests\data\sample.csv")
    t_filter = TransformOperator("test", "filter", Country="Cyprus")
    t_limit = TransformOperator("test", "limit", n=10)
    t_select = TransformOperator("test", "select", ["Last Name", "Country", "City","Phone 1","Email","Subscription Date"])
    write_op = WriteOperator("test", "csv", path= r"tests\data\output.csv", maintain_order=False)
    p = Pipeline(read_op, t_filter, t_limit, t_select, write_op)
    p.run()





