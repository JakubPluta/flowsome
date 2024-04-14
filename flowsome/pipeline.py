from typing import Any, Dict, Generator
import polars as pl
from flowsome.tasks import TaskNode
from flowsome.log import get_logger


log = get_logger(__name__)


class DAG:
    """
    Directed Acyclic Graph (DAG) - a collection of nodes, where each node has a task
    and references to its children and parents
    """
    def __init__(self):
        self.nodes: Dict[str, TaskNode] = {}
        
    def __repr__(self) -> str:
        return f"DAG(nodes={self.nodes})"
    
    def __iter__(self) -> Generator[TaskNode, None, None]:
        for node in self.nodes.values():
            yield node
    
    def __len__(self) -> int:
        return len(self.nodes)
    
    def __getitem__(self, task_id: str) -> TaskNode:
        """
        A function to retrieve a TaskNode based on the given task_id.

        :param task_id: The identifier of the task to retrieve.
        :type task_id: str
        :return: The TaskNode corresponding to the task_id.
        :rtype: TaskNode
        """
        try:
            return self.nodes[task_id]
        except KeyError as e:
            log.error(f"Task {task_id} not found in DAG")
            raise KeyError from e
        
    def add_node(self, task: TaskNode) -> None:
        """
        Add a new node to the graph if the node with the given task ID does not already exist.

        :param task: The task node to be added to the graph.
        :type task: TaskNode

        :return: None
        :rtype: None
        """
        if task.task_id not in self.nodes:
            self.nodes[task.task_id] = task

        
    def add_edge(self, parent: TaskNode, child: TaskNode) -> None:
        """
        Add an edge between a parent TaskNode and a child TaskNode.

        :param parent: The parent TaskNode.
        :type parent: TaskNode
        :param child: The child TaskNode.
        :type child: TaskNode

        :return: None
        :rtype: None
        """
        
        if parent.task_id not in self.nodes:
            self.add_node(parent)
        if child.task_id not in self.nodes:
            self.add_node(child)
        self.nodes[parent.task_id].add_child(child)
        self.nodes[child.task_id].add_parent(parent)



class Pipeline(DAG):
    """
    A Pipeline class that inherits from the DAG class and adds a run() method.
    """
    
    def __init__(self) -> None:
        super().__init__()
        self._artifacts = {}
        

    def __repr__(self) -> str:
        return "Pipeline(nodes={})".format(self.nodes)
    
    def run(self) -> None:
        """
        A function that runs the execution of the task nodes 
        and stores the results in a dictionary. 
        It starts the execution from the root nodes and recursively executes the child nodes. 
        It uses the results dictionary to store and retrieve the results of the executed nodes.
        """

        results: Dict[str, pl.LazyFrame] = {}
   
        def execute_node(node: TaskNode) -> None:
            """
            A function to execute a task node, considering its dependencies and storing the result.
            
            :param node: The task node to execute.
            :type node: TaskNode
            
            :return: None
            """
            if node.task_id not in results:
                # If node is not in results, check if it has parents that are in results
                parent_results = [results[parent.task_id] for parent in node.parents if parent.task_id in results]
                
                # If node has parents that are not in results, skip execution
                if len(node.parents) > len(parent_results):
                    return
                # if node has parents, ande all parents are in results execute node with parent results
                if parent_results:
                    node_result = node.execute(*parent_results)
                else:
                    node_result = node.execute()  # Execute without parent results (eg. read)
                    
                results[node.task_id] = node_result # Store result in results dict
                for child in node.children:
                    execute_node(self.nodes[child.task_id]) # Recursively execute child nodes

        # Start execution from root nodes
        for _, node in self.nodes.items():
            if node.is_root():
                execute_node(node)
