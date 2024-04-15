from typing import Dict, Generator, List
import polars as pl
from flowsome.tasks import TaskNode
from flowsome.log import get_logger


log = get_logger(__name__)


class PipelineCycleError(Exception):
    """
    Raised when a pipeline has a cycle in its dependency graph
    """


class DAG:
    """
    Directed Acyclic Graph (DAG) - a collection of nodes, where each node has a task
    and references to its children and parents
    """

    def __init__(self):
        """
        Initializes the Directed Acyclic Graph (DAG) object with an empty dictionary of nodes.
        """
        self.nodes: Dict[str, TaskNode] = {}

    def __repr__(self) -> str:
        """
        Returns a string representation of the DAG.
        """
        return f"DAG(nodes={self.nodes})"

    def __iter__(self) -> Generator[TaskNode, None, None]:
        """
        Iterates over the nodes in the DAG.

        :return: A generator that yields the nodes in the DAG.
        :rtype: Generator[TaskNode, None, None]
        """
        for node in self.nodes.values():
            yield node

    def __len__(self) -> int:
        """
        Returns the number of nodes in the Directed Acyclic Graph (DAG).

        :return: The number of nodes in the DAG.
        :rtype: int
        """
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
            log.error("Task %s not found in DAG", task_id)
            raise KeyError from e

    @property
    def has_cycle(self) -> bool:
        """
        Check if the DAG has a cycle.

        :return: True if the DAG has a cycle, False otherwise.
        :rtype: bool
        """
        return bool(self.find_cycles())

    def show_dependency_graph(self) -> None:
        """
        Show the dependency graph in the console.
        """
        for node in self.nodes.values():
            print(node)
            for child in node.children:
                print(f"    -> {child}")
            for parent in node.parents:
                print(f"    <- {parent}")
            print()

    def find_orphan_nodes(self) -> List[TaskNode]:
        """
        Find orphan nodes in the DAG.

        :return: A list of orphan nodes.
        :rtype: List[TaskNode]
        """
        orphan_nodes = [
            node
            for node in self.nodes.values()
            if not node.parents and not node.children
        ]
        if orphan_nodes:
            log.warning("Found %s orphan nodes in the DAG", len(orphan_nodes))
        return orphan_nodes

    def find_cycles(self) -> List[TaskNode]:
        """
        Find cycles in the DAG.

        :return: A list of nodes that form a cycle.
        :rtype: List[TaskNode]
        """

        def depth_first_search(node, visited, stack):
            """
            Perform a depth-first search (DFS) on the graph starting from the given node.

            :param node: The current node being visited.
            :type node: Any
            :param visited: A dictionary to keep track of visited nodes.
            :type visited: Dict[Any, bool]
            :param stack: A dictionary to keep track of nodes on the current traversal path.
            :type stack: Dict[Any, bool]

            :return: True if a cycle is found, False otherwise.
            :rtype: bool
            """
            visited[node] = True
            stack[node] = True
            for child in self.nodes[node].children:
                if not visited[child.task_id]:
                    if depth_first_search(child.task_id, visited, stack):
                        return True
                elif stack[child.task_id]:
                    return True
            stack[node] = False
            return False

        visited = {node_id: False for node_id in self.nodes}
        stack = {node_id: False for node_id in self.nodes}
        cycle_nodes = []

        for node_id in self.nodes:
            if not visited[node_id]:
                if depth_first_search(node_id, visited, stack):
                    cycle_nodes.append(self.nodes[node_id])
        if cycle_nodes:
            log.warning("Found %s cycles in the DAG", len(cycle_nodes))

        return cycle_nodes

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
        return f"Pipeline(nodes={self.nodes})"

    def run(self) -> None:
        """
        A function that runs the execution of the task nodes
        and stores the results in a dictionary.
        It starts the execution from the root nodes and recursively executes the child nodes.
        It uses the results dictionary to store and retrieve the results of the executed nodes.
        """

        if self.has_cycle:
            raise PipelineCycleError(
                "Pipeline has cycles. Please check the pipeline dependency graph."
            )

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
                parent_results = [
                    results[parent.task_id]
                    for parent in node.parents
                    if parent.task_id in results
                ]

                # If node has parents that are not in results, skip execution
                if len(node.parents) > len(parent_results):
                    return
                # if node has parents, ande all parents are in results execute node with parent results
                if parent_results:
                    node_result = node.execute(*parent_results)
                else:
                    node_result = (
                        node.execute()
                    )  # Execute without parent results (eg. read)

                results[node.task_id] = node_result  # Store result in results dict
                for child in node.children:
                    execute_node(
                        self.nodes[child.task_id]
                    )  # Recursively execute child nodes

        # Start execution from root nodes
        for _, node in self.nodes.items():
            if node.is_root():
                execute_node(node)
