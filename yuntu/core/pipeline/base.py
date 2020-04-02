"""Base class for audio processing pipelines."""
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
from yuntu.core.pipeline.nodes.base import Node
from yuntu.core.pipeline.nodes.inputs import Input
from yuntu.core.pipeline.nodes.operations import Operation
import uuid
import warnings


class Pipeline(ABC):
    """Base class for processing pipelines."""

    def __init__(self, name):
        self.name = name
        self.nodes = OrderedDict()
        self.deps = OrderedDict()

    def add_node(self, node):
        """Add node element to pipeline."""
        if not isinstance(node, Node):
            raise ValueError("Argument 'node' must be of class Node.")
        if node in self:
            raise ValueError(f"Node exists.")
        node.set_pipeline(self)
        key = node.name
        if key is not None:
            if key in self.nodes:
                key = f"{key}_{uuid.uuid1()}"
        else:
            key = str(uuid.uuid1())
        self.nodes[key] = node
        self.add_deps(key)

    def add_deps(self, key):
        """Add node dependencies to pipeline in case they are absent."""
        if key not in self.nodes:
            message = "Key not found."
            raise KeyError(message)
        deps = None
        if isinstance(self.nodes[key], Operation):
            deps = []
            for dep in self.nodes[key].inputs:
                if dep.name not in self.names:
                    dep.set_pipeline(self)
                    dep.attach()
                elif dep.key is not None:
                    if dep.key not in self.nodes:
                        dep.set_pipeline(self)
                        dep.attach()
                    elif dep.pipeline != self:
                        dep.set_pipeline(self)
                        dep.attach()
                if dep.key is None:
                    message = ("Automatic input assignation was not " +
                               f" safe for node with name '{dep.name}'." +
                               " Please use 'node.set_inputs' to explicitly " +
                               "assign inputs.")
                    warnings.warn(message)
                deps.append(dep.key)
        self.deps[key] = deps

    def get_deps(self, key):
        """Return node dependencies."""
        if key not in self.nodes:
            message = "Key not found."
            raise KeyError(message)
        if key not in self.deps:
            message = "Dependencies have not been assigned."
            raise KeyError(message)
        if self.deps[key] is None:
            return None
        return [self.nodes[dkey] for dkey in self.deps[key]]

    def remove_node(self, node):
        """Remove node."""
        if not isinstance(node, Node):
            if isinstance(node, str):
                key = self.node_key(node)
            else:
                raise TypeError("Item must be a pipeline node or a string.")
            self.nodes[key].clear()
            del self.nodes[key]
            del self.deps[key]
        else:
            if node not in self:
                raise ValueError("Node not found within this pipeline.")
            node.clear()
            del self.nodes[node.key]
            del self.deps[node.key]

    def node_key(self, index):
        """Return node key if argument makes sense as index or as key."""
        if len(self) == 0:
            raise IndexError("Pipeline is empty.")
        if isinstance(index, str):
            if index not in self.nodes:
                raise ValueError(f"Key {index} does not exist.")
            return index
        if isinstance(index, int):
            raise ValueError("Index must be an integer.")
        if index >= len(self):
            raise IndexError("Index must be smaller than pipeline length.")
        key = self.nodes.items()[index].key
        return key

    def node_index(self, key):
        """Return node index if argument makes sense as key or as index."""
        if len(self) == 0:
            raise IndexError("Pipeline is empty.")
        if isinstance(key, int):
            if key >= len(self):
                raise IndexError("If argument is integer, input value" +
                                 " must be lower than pipeline length.")
            return key
        if not isinstance(key, str):
            raise ValueError("Argument must be a string.")
        for i in range(len(self.nodes.items())):
            if self.nodes.items()[i][0] == key:
                return i
        raise KeyError(f"Key {key} does not exist")

    def node_name(self, key):
        """Return node name."""
        if len(self) == 0:
            raise IndexError("Pipeline is empty.")
        if isinstance(key, int):
            key = self.node_key(key)
        elif isinstance(key, str):
            if key not in self.nodes:
                raise KeyError("Node not found in this pipeline.")
        else:
            raise TypeError("Argument must be a string or an integer.")
        return self.nodes[key]

    @property
    def inputs(self):
        inputs = [key for key in self.nodes
                  if isinstance(self.nodes[key], Input)]
        for node in inputs:
            yield node

    @property
    def operations(self):
        operations = [key for key in self.nodes
                      if isinstance(self.nodes[key], Operation)]
        for node in operations:
            yield node

    @property
    def names(self):
        """Return iterator of names."""
        unique_names = list(set([self.nodes[key].name for key in self.keys()]))
        for name in unique_names:
            yield name

    @property
    def identifiers(self):
        """Return an iterator of tuples of the form (index, key, name)."""
        keys = list(self.keys())
        for i in range(len(keys)):
            yield i, keys[i], self.nodes[keys[i]].name

    def keys(self):
        """Return iterator of keys."""
        return self.nodes.keys()

    def __len__(self):
        """Return the number of pipeline nodes."""
        return len(self.nodes)

    def __getitem__(self, key):
        """Return node with key."""
        key = self.node_key(key)
        return self.nodes[key]

    def __delitem__(self, key):
        key = self.node_key(key)
        self.nodes[key].clear()
        del self.nodes[key]
        del self.deps[key]

    def __setitem__(self, key, value):
        """Set node with key to value."""
        if isinstance(key, int):
            if key >= len(self):
                raise IndexError("Setting a new node by index is " +
                                 "not allowded. Use a string or index " +
                                 "within range.")
            key = self.node_key(key)
        if not isinstance(value, Node):
            if key not in self.nodes:
                raise KeyError("Key not found. Setting node result value is" +
                               " only allowded for existing nodes.")
            self.nodes[key].set_value(value)
        else:
            if value in self:
                if value.key != key:
                    for node_key in self.deps:
                        if self.deps[node_key] is not None:
                            for i in range(len(self.deps[node_key])):
                                if self.deps[node_key][i] == value.key:
                                    self.deps[node_key][i] = key
                    former_key = value.key
                    self.nodes[key] = self.nodes.pop(former_key)
                    self.deps[key] = self.deps.pop(former_key)
            else:
                value.set_pipeline(self)
                self.nodes[key] = value
                self.add_deps(key)


    def __iter__(self):
        """Return node iterator."""
        for key in self.nodes:
            yield key

    def __contains__(self, node):
        """Return true if item in pipeline."""
        if isinstance(node, Node):
            return node in [item[1] for item in self.nodes.items()]
        return node in self.nodes

    @abstractmethod
    def get_node(self, key, compute=False, force=False, **kwargs):
        """Get node from pipeline graph."""

    @abstractmethod
    def compute(self, nodes=None, force=False, **kwargs):
        """Compute pipeline."""
