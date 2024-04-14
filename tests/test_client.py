from flowsome.pipeline import Pipeline
from flowsome.tasks import *


def test_single_pipeline():
    pipe = Pipeline()
    reader = ReadTask("r1", "csv", source=r"tests/data/sample.csv")
    transformer1 = TransformTask("t1", "filter", Country="Cyprus")
    transformer2 = TransformTask("t2", "limit", n=1)
    writer = WriteTask("w1", "csv", path="output.csv")
    pipe.add_edge(reader, transformer1)
    pipe.add_edge(transformer1, transformer2)
    pipe.add_edge(transformer2, writer)
    pipe.run()
    assert pipe
    
    
    