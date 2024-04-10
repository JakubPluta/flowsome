from copy import copy
from flowsome.tasks import Task, TaskType, ReadTask, MergeTask, WriteTask, TransformTask
from typing import List, Any, Dict, Tuple
from flowsome.log import get_logger


log = get_logger(__name__)


class Pipeline:
    
    def __init__(self) -> None:
        self._nodes: List[Task] = []
        
    def __len__(self):
        return len(self._nodes)
    
    def __repr__(self) -> str:
        return f"Pipeline(nodes={self._nodes})"
    
    def __iter__(self):
        for node in self._nodes:
            yield from self.iter_from_node(node)
            
    def __contains__(self, task: Task | str) -> bool:
        return self.contains_node(task)
    
    @property
    def nodes(self) -> List[Task]:
        return list(self._nodes)
            
    def contains_node(self, task: Task | str) -> bool:
        if isinstance(task, str):
            return self.find_node(task) is not None
        return task in self._nodes
    
    
    def iter_from_node(self, node: Task):
        while node.successor:
            yield node 
            node = node.successor
        yield node
            
    def iter_tasks(self):
        for node in self._nodes:
            yield from self.iter_from_node(node)

    def list_pipelines(self):
        return [list(self.iter_from_node(node)) for node in self._nodes]
    
    def list_tasks(self):
        tasks = copy(self._nodes)
        for node in self._nodes:
            while node.successor:
                node = node.successor
                tasks.append(node)
        return list(set(tasks))
    
    def find_task(self, task_id: str) -> Task:
        for node in self._nodes:
            for task in self.iter_from_node(node):
                if task.task_id == task_id:
                    return task 
        raise LookupError("couldn't find task in nodes, and all downstreams")
    
    def find_node(self, task_id: str) -> Task:
        for node in self._nodes:
            if node.task_id == task_id:
                return node
        raise LookupError("couldn't find task in nodes")
        
    
    def add_node(self, task: Task) -> None:
        if task in self._nodes:
            raise ValueError("%s already added", task)
        self._nodes.append(task)
        
    def remove_node(self, task_id: str) -> None:
        found = False
        for node in self._nodes:
            if node.task_id == task_id:
                found = True
                self._nodes.remove(node)
        if found is False:
            raise ValueError("couldn't delete task %s - doesn't exists in nodes list")
            
    
    def add_downstream(self, task: Task, downstream_task: Task):
        if task not in self.list_tasks():
            raise ValueError("parent task not exists in ")
        
        for dtask in self.iter_from_node(task):
            if dtask.task_id == downstream_task.task_id:
                raise ValueError("downstream %s already in added to task %s", downstream_task, task)
        task.add_successor(downstream_task)


    def add_upstream(self, task: Task, upstream_task: Task):
        if not self.contains_node(task):
            raise ValueError("Parent task not exists in nodes list")
        
        for dtask in self.iter_from_node(task):
            if dtask.task_id == upstream_task.task_id:
                raise ValueError("upstream task %s alread a downstream to task %s", upstream_task, task)
    
        for node in self._nodes:
            if node.task_id == task.task_id:
                self._nodes.pop(task)
                break 
        
        upstream_task.add_successor(task)
        self._nodes.append(upstream_task)



class PipelineRunner:
    
    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.execution_plans = self.pipeline.list_pipelines()
    
    def is_simple_pipeline(self) -> bool:
        return len(self.execution_plans) == 1
    
    def _execution_plans_are_equal(self, list_of_lists: List[List[Task]]) -> bool:
        return all(list_of_lists[i] == list_of_lists[i + 1] for i in range(len(list_of_lists) - 1))
    
    @staticmethod
    def _split_list_by_value(input_list: List[Any], split_value: List[Any]) -> Tuple[List[Any], List[Any]]:
        list1, list2 = None, None
        try:
            split_index = input_list.index(split_value)
            list1 = input_list[:split_index]
            list2 = input_list[split_index:]
        except ValueError:
            log.info("Couldn't find split value in list")
        return list1, list2
    
    def _split_execution_plans(self, execution_plans: List[List[Task]], common_task: Task) -> Tuple[List[List[Task]], List[List[Task]]]:
        plans_before_merge, plans_after_merge = [], []
        for plan in execution_plans:
            before_merge, after_merge = self._split_list_by_value(plan, common_task)
            plans_before_merge.append(before_merge)
            plans_after_merge.append(after_merge)
        return plans_before_merge, plans_after_merge

    def _find_first_common_task(self) -> Task | None:
        if not self.execution_plans:
            return None
        
        # Get the tasks from the first plan
        first_plan_tasks = self.execution_plans[0]

        # Iterate through the tasks in the first plan
        for task in first_plan_tasks:
            # Check if the task is present in all other plans
            if all(task in plan for plan in self.execution_plans[1:]):
                return task
        return None
     
    
    def run(self):
        if len(self.pipeline) > 2:
            raise ValueError("Pipeline with more than 2 nodes is not supported")
        
        if self.is_simple_pipeline():
            execution_plan = self.execution_plans[0]
            reader = execution_plan.pop(0)
            df = reader.execute()
            for task in execution_plan:
                df = task.execute(df)
        else:
            first_common_task = self._find_first_common_task()
            if first_common_task is None:
                raise ValueError("No common task found")
            plans_before_merge, plans_after_merge = self._split_execution_plans(self.execution_plans, first_common_task)
            
            if not self._execution_plans_are_equal(plans_after_merge):
                raise ValueError("Execution plans after merge are not equal")
            plan_after_merge = plans_after_merge[0]
            
            dfs = []
            for pipe in plans_before_merge:
                reader = pipe.pop(0)
                df = reader.execute()
                for task in pipe:
                    df = task.execute(df)
                dfs.append(df)
            merge_task: MergeTask = plan_after_merge.pop(0)
            if not merge_task.is_merge_task():
                raise ValueError("first task after fan-in should be a merge task")
            df = merge_task.execute(*dfs)
            for task in plan_after_merge:
                df = task.execute(df)
            return df
