from copy import copy
from flowsome.tasks import TaskNode, TaskType, ReadTask, MergeTask, WriteTask, TransformTask
from typing import List, Any, Dict, Tuple
from flowsome.log import get_logger


log = get_logger(__name__)


class DAG:
    """
    Directed Acyclic Graph (DAG)

    Each DAG is a collection of nodes, where each node has a task
    and references to its children and parents
    """
    def __init__(self):
        self.nodes = {}
        
    def __repr__(self) -> str:
        return f"DAG(nodes={self.nodes})"

    def add_node(self, task: TaskNode):
        """Add a node to the DAG"""
        if task.task_id not in self.nodes:
            self.nodes[task.task_id] = task

        
    def add_edge(self, parent: TaskNode, child: TaskNode):
        """Add an edge between two nodes in the DAG"""
        if parent not in self.nodes:
            self.add_node(parent)
        if child not in self.nodes:
            self.add_node(child)
        self.nodes[parent.task_id].add_child(child)
        self.nodes[child.task_id].add_parent(parent)

    def run(self):
        """
        Run the DAG by traversing nodes, executing code, and passing results to the next nodes
        """
        results = {}
   
        def execute_node(node):
            """Execute the code associated with a node and pass results from previous tasks"""
            if node.task_id not in results:
      
                parent_results = [results[parent.task_id] for parent in node.parents if parent.task_id in results]
                if len(node.parents) > len(parent_results):
                    # Node has parents whose results are not available, skip execution
                    return
                if parent_results:
                    node_result = node.execute(*parent_results)
                else:
                    node_result = node.execute()  # Execute without parent results
                results[node.task_id] = node_result
                for child in node.children:
                    execute_node(self.nodes[child.task_id])

        for _, node in self.nodes.items():
            if node.is_root():
                execute_node(node)
        return results