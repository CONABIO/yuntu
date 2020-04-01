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
        self._key = None
        self._index = None
        self._result = None

    @property
    def key(self):
        if self.pipeline is None:
            self._key = None
        elif self._key is not None:
            if self._key not in self.pipeline:
                self._key = None
            elif self.pipeline[self._key] != self:
                self._key = None
        else:
            for key in self.pipeline:
                if self.pipeline[key] == self:
                    self._key = key
        return self._key

    @property
    def index(self):
        if self.pipeline is None:
            self._index = None
        elif self._index is not None:
            if self._index >= len(self.pipeline):
                self._index = None
            elif self.pipeline[self._index] != self:
                self._index = None
        else:
            for index, node in self.pipeline.nodes.items():
                if node == self:
                    self._index = index
        return self._index

    def set_value(self, value):
        if not self.validate(value):
            raise ValueError("Value is incompatible with node type " +
                             f"{type(self)}")
        self._result = value

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline
        self._key = None
        self._index = None

    def is_persisted(self):
        return os.path.exists(self.get_persist_path())

    def clear(self):
        if self.is_persisted():
            os.remove(self.get_persist_path())
        self._result = None

    def attach(self):
        """Attach self to pipeline."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        if self.key not in self.pipeline:
            self.pipeline.add_node(self)

    @abstractmethod
    def __copy__(self):
        """Return a shallow copy of self."""

    @abstractmethod
    def validate(self, data):
        """Validate data."""

    @abstractmethod
    def get_persist_path(self):
        """Path to operation persisted outputs."""

    @abstractmethod
    def read(self, path, **kwargs):
        """Read node from path."""

    @abstractmethod
    def write(self, path, **kwargs):
        """Write node to path."""
