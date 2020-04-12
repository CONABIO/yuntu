"""Base class for all pipeline nodes."""
from abc import ABC
from abc import abstractmethod
import os


class Node(ABC):
    """Pipeline node."""
    data_class = None
    node_type = "abstract"
    is_output = False
    is_transition = False
    is_place = False

    def __init__(self, name=None, pipeline=None):
        if name is not None:
            if not isinstance(name, str):
                message = "Node name must be a string."
                raise ValueError(message)
        self.name = name
        self.pipeline = pipeline
        self._key = None
        self._index = None
        self._result = None

    @property
    def key(self):
        """Get key within pipeline if exists."""
        self.refresh_key()
        return self._key

    @property
    def index(self):
        """Get index within pipeline if exists."""
        self.refresh_index()
        return self._index

    @property
    def meta(self):
        meta = {"key": self.key,
                "name": self.name,
                "node_type": self.node_type}
        return meta

    @abstractmethod
    def is_compatible(self, other):
        """Check if nodes is compatible with self for replacement."""

    def refresh_index(self):
        """Retrieve index from pipeline if exists and update."""
        if self.pipeline is None:
            self._index = None
        elif self._index is not None:
            if self._index >= len(self.pipeline):
                self._index = None
            elif self.pipeline[self._index] != self:
                self._index = None
        else:
            keys = list(self.pipeline.nodes.keys())
            for i in range(len(keys)):
                if self.pipeline.nodes[keys[i]] == self:
                    self._index = i

    def refresh_key(self):
        """Retrieve key from pipeline if exists and update."""
        if self.pipeline is None:
            self._key = None
        elif self._key is not None:
            if self._key not in self.pipeline.nodes:
                self._key = None
            elif self.pipeline.nodes[self._key] != self:
                self._key = None
        else:
            for key in self.pipeline.keys():
                if self.pipeline.nodes[key] == self:
                    self._key = key
                    break

    def set_value(self, value):
        """Set result value manually."""
        if not self.validate(value):
            raise ValueError("Value is incompatible with node type " +
                             f"{type(self)}")
        self._result = value

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline
        self._key = None
        self._index = None

    def attach(self):
        """Attach self to pipeline."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        if self.key not in self.pipeline:
            self.pipeline.add_node(self)
            self.refresh_key()
            self.refresh_index()
        elif self.pipeline[self.key] != self:
            self.pipeline[self.key] = self

    @abstractmethod
    def __copy__(self):
        """Return a shallow copy of self."""

    def validate(self, data):
        """Validate data."""
        if data is None:
            return True
        if self.data_class is None:
            return True
        return isinstance(data, self.data_class)

    @abstractmethod
    def future(self,
               feed=None,
               read=None,
               force=False,
               **kwargs):
        """Return node's future."""

    @abstractmethod
    def compute(self,
                feed=None,
                read=None,
                write=None,
                keep=None,
                force=False,
                **kwargs):
        """Compute self."""
