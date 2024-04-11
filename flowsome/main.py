from flowsome.pipeline import DAG
from flowsome.tasks import *

import polars as pl 

if __name__ == "__main__":
    
    dag = DAG()

    r1 = ReadTask("r1", "csv", source=r"tests/data/sample.csv")
    t1 = TransformTask("t1", "filter", Country="Cyprus")
    w1 = WriteTask("w1", "csv", path="output.csv")

    r2 = ReadTask("r2", "csv", source=r"tests/data/sample.csv")
    t2 = TransformTask("t2", "limit", n=44)

    m = MergeTask("m", on="Email", how="right")
    t3 = TransformTask("t3", "limit", n=41)

    r3 = ReadTask("r3", "csv", source=r"tests/data/sample.csv")
    t4 = TransformTask("t4", "limit", n=25)
    m2 = MergeTask("m2", on="Email", how="right", suffix="m2")

    dag.add_edge(r1, t1)
    dag.add_edge(t1, m)

    dag.add_edge(r2, t2)
    dag.add_edge(t2, m)

    dag.add_edge(m, t3)
    dag.add_edge(t3, m2)

    dag.add_edge(r3, t4)
    dag.add_edge(t4, m2)
    dag.add_edge(m2, w1)


    dag.run()