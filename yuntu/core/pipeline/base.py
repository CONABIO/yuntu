"""Base class for audio processing pipelines."""
from abc import ABC
from abc import abstractmethod


class Pipeline(ABC):
    """Base class for processing pipelines."""

    def __init__(self, name):
        self.name = name
        self.persist = []
        self.outputs = []

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
    def get_node(self, name):
        """Get node from pipeline graph."""

    @abstractmethod
    def compute(self, nodes=None, force=False):
        """Compute pipeline."""

    @abstractmethod
    def write_node(self, nodes=None):
        """Persist computations as files."""

    @abstractmethod
    def read_node(self, nodes=None):
        """Read computations from files."""

    def _mark_output(self, name):
        """Mark as output."""
        self._mark_persist(name)
        if name not in self.outputs:
            self.outputs.append(name)

    def _mark_persist(self, name):
        """Mark to be persisted when computed."""
        if name not in self.persist:
            self.persist.append(name)
