"""Base class for all pipeline nodes."""
from abc import ABC
from abc import abstractmethod
import os


class Node(ABC):
    """Pipeline node."""
    def __init__(self, name=None, pipeline=None):
        if name is not None:
            self.name = name
        else:
            self.name = 'node'
        self.pipeline = pipeline

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline

    def is_persisted(self):
        return os.path.exists(self.get_persist_path())

    def clear(self):
        if self.is_persisted():
            os.remove(self.get_persist_path())

    def attach(self):
        """Attach self to pipeline."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        self.pipeline.set_node(self)

    @abstractmethod
    def __copy__(self):
        """Return a shallow copy of self."""

    @abstractmethod
    def get_persist_path(self):
        """Path to operation persisted outputs."""

    @abstractmethod
    def read(self, path, **kwargs):
        """Read node from path."""

    @abstractmethod
    def write(self, path, **kwargs):
        """Write node to path."""
