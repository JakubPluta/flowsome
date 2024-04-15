from flowsome.pipeline import Pipeline
from flowsome.tasks import WriteTask, ReadTask, TransformTask, MergeTask

if __name__ == "__main__":
    p = Pipeline()

    r1 = ReadTask("r1", source=r"tests/data/sample.csv")
    t1 = TransformTask("t1", "filter", Country="Cyprus")
    w1 = WriteTask("w1", file_path="output.csv")

    r2 = ReadTask("r2", source=r"tests/data/sample.csv")
    t2 = TransformTask("t2", "limit", n=44)

    m = MergeTask("m", on="Email", how="right")
    t3 = TransformTask("t3", "limit", n=41)

    r3 = ReadTask("r3", source=r"tests/data/sample.csv")
    t4 = TransformTask("t4", "limit", n=3)
    m2 = MergeTask("m2", on="Email", how="right", suffix="m2")

    p.add_edge(r1, t1)
    p.add_edge(t1, m)

    p.add_edge(r2, t2)
    p.add_edge(t2, m)

    p.add_edge(m, t3)
    p.add_edge(t3, m2)

    p.add_edge(r3, t4)
    p.add_edge(t4, m2)
    p.add_edge(m2, w1)

    p.run()
