"""Collection of tools for pipeline management."""
from copy import copy
from yuntu.core.pipeline.node.base import Node
from yuntu.core.pipeline.node.extended import _guess_input_class


def input_node(data, name=None, pipeline=None):
    """Return input node according to data input class."""
    input_class = _guess_input_class(data)
    return input_class(name=name, data=data, pipeline=pipeline)


def are_compatible(node1, node2):
    """Check if nodes are compatible to be replaced."""
    if node1.node_type == "trasition" and node2.node_type == "transition":
        return node1.signature == node2.signature
    if node1.node_type == "place" and node2.node_type == "place":
        return isinstance(node1, node2.__class__)
    return False


def knit(*nodes):
    """Knit nodes into a single pipeline."""
    for ind, node in enumerate(nodes):
        if not isinstance(node, Node):
            message = f"Input {ind} is not a pipeline node."
            raise TypeError(message)

    pipelines = []
    for node in nodes:
        if node.pipeline not in pipelines:
            pipelines.append(node.pipeline)

    new_pipeline = copy(pipelines[0])

    for ind, pipeline in enumerate(pipelines):
        if ind > 0:
            new_pipeline = new_pipeline & pipeline

    str_names = ",".join([node.name if node.name is not None
                          else 'NoName'for node in nodes])
    new_pipeline.name = f"knit({str_names})"

    return new_pipeline
