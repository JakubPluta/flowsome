from flowsome.pipeline import Pipeline, PipelineOrchestrator
from flowsome.tasks import *

import polars as pl 

if __name__ == "__main__":
    
    po = PipelineOrchestrator()
    po.add_task("Root1", TaskType.read, fmt="csv", source=r"tests\data\sample.csv")
    po.add_task("Task1", TaskType.transform, "Root1", "filter", Country="Cyprus")



    po.add_task("Root2", TaskType.read, fmt="csv", source=r"tests\data\sample.csv")
    po.add_task("Task2", TaskType.transform, "Root2", "limit", n=33)


    po.add_task("Merge", TaskType.merge, ["Task1", "Task2"], "join", on="Email", how="right")
    po.add_task("Task3", TaskType.transform, "Merge", "limit", n=23)
    po.add_task("Writer", TaskType.write, "Task3", "csv", path=r"tests\data\output.csv", maintain_order=False)


    po.execute()


