from flowsome.pipeline import Pipeline, PipelineRunner
from flowsome.tasks import *

import polars as pl 

if __name__ == "__main__":
    
    pipe = Pipeline()
    
    task1 = ReadTask("read1", "csv", source=r"tests/data/sample.csv")
    task2 = TransformTask("transform1", "filter", Country="Cyprus")
    task3 = TransformTask("transform2",  "limit", n=33)
    task4 = WriteTask("write", "csv" , path=r"tests\data\output.csv")

    read2 = ReadTask("read2", "csv", source=r"tests/data/sample.csv")
    task5 = TransformTask("transform3", "limit", n=63)

    merge_task = MergeTask("merge", "join", on="Email", how="right")

    pipe.add_node(task1)
    pipe.add_downstream(task1, task2)
    pipe.add_downstream(task2, task3)
    pipe.add_node(read2)
    pipe.add_downstream(read2, task5)
    pipe.add_downstream(task3, merge_task)
    pipe.add_downstream(task5, merge_task)
    pipe.add_downstream(merge_task, task4)


    runner = PipelineRunner(pipeline=pipe)
    runner.run()
    
