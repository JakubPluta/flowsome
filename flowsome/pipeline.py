from __future__ import annotations
import enum
from typing import List, Optional

from collections import OrderedDict, deque
import enum
from functools import cached_property
from typing import Any, List, Sequence
import polars as pl
from flowsome.utils import get_scan_method, get_sink_method
from flowsome.log import get_logger
from flowsome.tasks import Task, TaskType, ReadTask, WriteTask, MergeTask, TransformTask


log = get_logger(__name__)


class Pipeline:
    def __init__(self) -> None:
        self._tasks: List[Task] = []
        
    def __repr__(self) -> str:
        return f"Pipeline(tasks={self._tasks})"
    
    def get_task(self, task_id: str) -> Task | None:
        try:
            return next(task for task in self._tasks if task.task_id == task_id)
        except StopIteration:
            return None
  
    
    def is_task_in_pipeline(self, task_id: str) -> bool:
        return self.get_task(task_id) is not None
    
    def is_succesor_of_task(self, task: Task, successor: Task) -> bool:
        return successor == task.successor
    
    
    def add_task(self, task: Task):
        if self.is_task_in_pipeline(task.task_id):
            raise Exception(f"Task: {task.task_id} already in pipeline")
        if task.is_merge_task() and self.is_merge_task_in_pipeline():
            raise Exception("Merge task already in pipeline. Only one merge task is allowed")
        self._tasks.append(task)
    
    def add_successor(self, task: Task | str, successor: Task):
        if self.is_succesor_of_task(task, successor):
            raise Exception(f"Task: {successor.task_id} already in successors list")
        
        if task is successor:
            raise Exception(f"Task: {successor.task_id} cannot be successor of itself")
        task.add_successor(successor)
        self._tasks.append(successor)
        
        
    def iter_successors(self, task: Task):
        while task.successor:
            task = task.successor
            yield task
                
    def find_root_tasks(self) -> List[ReadTask]:
        return [task for task in self._tasks if task.is_root()]
    

    def is_merge_task_in_pipeline(self) -> bool:
        for task in self._tasks:
            if task._type == TaskType.merge:
                return True
        return False



class PipelineOrchestrator:
    task_registry = {
        TaskType.read: ReadTask,
        TaskType.transform: TransformTask,
        TaskType.write: WriteTask,
        TaskType.merge: MergeTask
    }
    def __init__(self) -> None:
        self.pipeline = Pipeline()
        
    
    def _create_task(self, task_name: str, task_type: TaskType, *args, **params) -> Task:
        try:
            return self.task_registry[task_type](task_name, *args, **params)
        except (KeyError, AttributeError) as e:
            log.error("Task type %s not found", task_type)
            raise AttributeError from e

        
    def add_task(self, task_name: str, task_type: TaskType, predecessor_name: str | List = None, *args, **params) -> Task:
        predecessor_tasks = None
        if predecessor_name and isinstance(predecessor_name, list):
            predecessor_tasks = [self.pipeline.get_task(predecessor_name) for predecessor_name in predecessor_name]
            if not all(isinstance(predecessor_task, Task) for predecessor_task in predecessor_tasks):
                raise ValueError("Predecessor task for merge must be a list of tasks")
        elif predecessor_name and isinstance(predecessor_name, str):
            predecessor_tasks = [self.pipeline.get_task(predecessor_name)]
            
        task = self._create_task(task_name, task_type, *args, **params)
        
        if predecessor_tasks:
            for predecessor_task in predecessor_tasks:
                predecessor_task.add_successor(task)
        self.pipeline.add_task(task)
        return task

    def _execute_single_pipeline(self, read_task: ReadTask):
        task: ReadTask = read_task
        df: pl.LazyFrame = task.execute()
        while task.successor:
            if task.is_merge_task():
                break
            task = task.successor
            df = task.execute(df)
            return df
    
    
    def _execute_from_merge_task(self, merge_task: MergeTask, df: pl.LazyFrame, other_df: pl.LazyFrame):
        results = merge_task.execute(df, other_df)
        while merge_task.successor:
            merge_task = merge_task.successor
            results = merge_task.execute(results)
        return results

    def _find_merge_task(self) -> MergeTask:
        return [task for task in self.pipeline._tasks if task.is_merge_task()][0]
    
    
    def execute(self):    
        root_tasks = self.pipeline.find_root_tasks()
        if len(root_tasks) == 0:
            raise Exception("No root tasks found")
        if len(root_tasks) > 2:
            raise Exception("More than two root tasks found. Only one or twoo root tasks are supported")
        
        if len(root_tasks) == 1:
            self._execute_single_pipeline(root_tasks[0])
        else:
            dfs = []
            merge_task = self._find_merge_task()
            if merge_task is None:
                raise Exception("No merge task found in pipeline")
            for root_task in root_tasks:
                df = self._execute_single_pipeline(root_task)
                dfs.append(df)
            self._execute_from_merge_task(merge_task, *dfs)
            
 
        
        

