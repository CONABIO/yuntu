"""Base class for audio processing pipelines."""
from abc import ABC
from abc import abstractmethod
from yuntu.core.pipeline.nodes import Node


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
        node.set_pipeline(self)

    def add_node(self, node):
        """Add node element to pipeline."""
        if not isinstance(node, Node):
            raise ValueError("Argument 'node' must be of class Node.")
        if node.name not in self.nodes:
            node.set_pipeline(self)

    @abstractmethod
    def add_operation(self,
                      name,
                      operation,
                      inputs=None,
                      is_output=False,
                      persist=False):
        """Add operation."""

    @abstractmethod
    def add_input(self, name, data):
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
