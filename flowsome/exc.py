"""Module for custom exceptions."""


class PipelineCycleError(Exception):
    """
    Raised when a pipeline has a cycle in its dependency graph
    """


class TaskExecutionError(Exception):
    """
    Raised when an error occurs while executing a task.
    """
