"""Base class for audio processing pipelines."""
from abc import ABC
from abc import abstractmethod
from yuntu.core.pipeline.nodes.base import Node
from yuntu.core.pipeline.nodes.inputs import Input
from yuntu.core.pipeline.nodes.operations import Operation


class Pipeline(ABC):
    """Base class for processing pipelines."""

    def __init__(self, name):
        self.name = name
        self.nodes = {}
        self.persist = []
        self.outputs = []

    def set_node(self, node):
        """Add node element to pipeline."""
        if not isinstance(node, Node):
            raise ValueError("Argument 'node' must be of class Node.")
        if isinstance(node, Input):
            node.set_pipeline(self)
            self._add_input(node.name, node.data)
            self.nodes[node.name] = node
        elif isinstance(node, Operation):
            node.set_pipeline(self)
            self._add_operation(node.name,
                                node.operation,
                                node.inputs,
                                node.is_output,
                                node.persist)
            self.nodes[node.name] = node
        else:
            raise NotImplementedError("Node type not implemented.")

    def knit_inputs(self, knit_map):
        """Reduce input nodes by mapping."""
        seen = []
        for cat1 in knit_map:
            for cat2 in knit_map:
                if cat1 != cat2:
                    if bool(set(knit_map[cat1]) & set(knit_map[cat2])):
                        raise ValueError("Knitting elements must be disjoint.")



    def add_node(self, node):
        """Add node element to pipeline."""
        if not isinstance(node, Node):
            raise ValueError("Argument 'node' must be of class Node.")
        if node.name in self.nodes:
            raise ValueError("A node with the same name already exists. Use" +
                             " 'setNode' or to overwrite or change the name.")

        if isinstance(node, Input):
            node.set_pipeline(self)
            self._add_input(node.name, node.data)
            self.nodes[node.name] = node
        elif isinstance(node, Operation):
            node.set_pipeline(self)
            self._add_operation(node.name,
                                node.operation,
                                node.inputs,
                                node.is_output,
                                node.persist)
            self.nodes[node.name] = node
        else:
            raise NotImplementedError("Node type not implemented.")

    def remove_node(self, node):
        """Remove node."""
        if not isinstance(node, Node):
            if isinstance(node, str):
                self.nodes[node].clear()
                del self.nodes[node]
            else:
                raise TypeError("Item must be a pipeline node or a string.")
        else:
            self.nodes[node.name].clear()
            del self.nodes[node.name]

    def keys(self):
        return self.nodes.keys()

    def __len__(self):
        """Return the number of pipeline nodes."""
        return len(self.nodes)

    def __getitem__(self, key):
        """Return node with key."""
        if not isinstance(key, str):
            raise TypeError("Key must be a string.")
        if len(self) == 0:
            raise KeyError("Pipeline is empty.")
        if key not in self.nodes:
            raise KeyError("Node not found in this pipeline.")
        return self.nodes[key]

    def __delitem__(self, key):
        if not isinstance(key, str):
            raise TypeError("Key must be a string.")
        if key not in self.nodes[key]:
            raise KeyError("Node not found within pipeline.")
        self.nodes[key].clear()
        del self.nodes[key]

    def __setitem__(self, key, value):
        """Set node with key to value."""
        if not isinstance(key, str):
            raise TypeError("Key must be a string.")
        if not isinstance(value, Node):
            raise TypeError("Value to set must be a pipeline node.")
        if value.name != key:
            value.name = key
        self.set_node(value)

    def __iter__(self):
        """Return node iterator."""
        for key in self.nodes:
            yield key

    def __contains__(self, item):
        """Return true if item in pipeline."""
        if not isinstance(item, Node):
            if isinstance(item, str):
                return item in self.nodes
            raise TypeError("Item must be a pipeline node or a string.")
        return item.name in self.nodes

    @abstractmethod
    def _add_operation(self,
                       name,
                       operation,
                       inputs=None,
                       is_output=False,
                       persist=False):
        """Add operation."""

    @abstractmethod
    def _add_input(self, name, data):
        """Add input."""

    @abstractmethod
    def get_node(self, name, compute=False, force=False):
        """Get node from pipeline graph."""

    @abstractmethod
    def compute(self, nodes=None, force=False, **kwargs):
        """Compute pipeline."""

    @abstractmethod
    def write_node(self, nodes=None):
        """Persist computations as files."""

    @abstractmethod
    def read_node(self, nodes=None):
        """Read computations from files."""

    def node_exists(self, name):
        """Check if node exists."""
        return name in self.nodes

    def _mark_output(self, name):
        """Mark as output."""
        self._mark_persist(name)
        if name not in self.outputs:
            self.outputs.append(name)

    def _mark_persist(self, name):
        """Mark to be persisted when computed."""
        if name not in self.persist:
            self.persist.append(name)
