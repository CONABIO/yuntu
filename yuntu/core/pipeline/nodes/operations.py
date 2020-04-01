"""Operation pipeline nodes."""
from abc import ABC
from abc import abstractmethod
import os
from yuntu.core.pipeline.nodes.base import Node
import dask.dataframe as dd
import pandas as pd
from copy import copy


class Operation(Node, ABC):
    """Pipeline operation base node."""
    output_class = None

    def __init__(self,
                 *args,
                 operation=None,
                 inputs=None,
                 is_output=False,
                 persist=False,
                 keep=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(operation, "__call__"):
            message = "Argument 'operation' must be a callable object."
        if not isinstance(inputs, (list, tuple)):
            message = ("Argument 'inputs' must be a list or a " +
                       "tuple of nodes.")
            raise ValueError(message)
        if inputs is not None:
            for node in inputs:
                if not isinstance(node, Node):
                    message = ("At least one of the inputs is not a node.")
                    raise ValueError(message)
            self._inputs = inputs
        else:
            self._inputs = []
        self.operation = operation
        self.is_output = is_output
        self.persist = persist
        self.keep = keep
        self._result = None
        if self.pipeline is None:
            self.pipeline = self._inputs_pipeline()
        if self.pipeline is not None:
            self.attach()

    def set_pipeline(self, pipeline):
        """Set pipeline for self and dependencies if needed."""
        self.pipeline = pipeline

    def _inputs_pipeline(self):
        """Try to guess pipeline from initial inputs."""
        pipeline = None
        for node in self._inputs:
            if node.pipeline is not None:
                if pipeline is None:
                    pipeline = node.pipeline
                elif pipeline != node.pipeline:
                    raise ValueError("Inputs have different pipelines.")
        return pipeline

    @property
    def inputs(self):
        if self.pipeline is None or self.key is None:
            return [node for node in self._inputs]
        elif self.key not in self.pipeline.deps:
            return [node for node in self._inputs]
        return self.pipeline.get_deps(self.key)

    def set_inputs(self, inputs):
        """Set hard value for inputs (when pipeline is None)"""
        if not isinstance(inputs, (tuple, list)):
            raise ValueError("Argument must be a list.")
        for node in inputs:
            if not isinstance(node, Node):
                raise ValueError("All elements must be nodes.")
        self._inputs = inputs
        if self.pipeline is not None and self.key is not None:
            for i in range(len(self._inputs)):
                if self._inputs[i].pipeline != self.pipeline:
                    self._inputs[i].set_pipeline(self.pipeline)
                    if self._inputs[i].key not in self.pipeline:
                        self._inputs[i].attach()
                self.pipeline.deps[self.key][i] = self._inputs[i].key

    def attach(self):
        """Attach self to pipeline."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        if self.key not in self.pipeline:
            self.pipeline.add_node(self)
        elif self.pipeline[self.key] != self:
            self.pipeline[self.key] = self

    def validate(self, data):
        """Validate data according to output type."""
        return isinstance(data, self.output_class)

    @abstractmethod
    def compute(self, force=False, client=None, dask_config=None):
        """Compute self."""


class DaskOperation(Operation, ABC):
    """Dask operation node."""
    output_class = None

    def compute(self, force=False, client=None, dask_config=None):
        """Compute self."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'attach'."
            raise ValueError(message)
        if not force and self._result is not None:
            return self._result
        result = self.pipeline.compute([self.name],
                                       force=force,
                                       client=None,
                                       dask_config=dask_config)[self.name]
        if self.keep:
            self._result = result
        return result


class DaskDataFrameOperation(DaskOperation):
    """Dask dataframe operation node."""
    output_class = (dd.core.DataFrame, pd.DataFrame)

    def write(self, path=None, data=None):
        if path is None:
            path = self.get_persist_path()
        if data is not None:
            if not isinstance(data, (pd.DataFrame,
                                     dd.core.DataFrame)):
                message = "Argument 'data' must be a dask or pandas dataframe."
                raise ValueError(message)
            results = data
        elif self._result is not None:
            results = self._result
        else:
            message = "No results yet. Compute node first."
            raise ValueError(message)
        return results.to_parquet(self.get_persist_path(), compression="GZIP")

    def read(self, path=None):
        if path is None:
            path = self.get_persist_path()
        if not os.path.exists(path):
            message = "No operation results at path."
            raise ValueError(message)
        results = dd.read_parquet(self.get_persist_path())
        if self.keep:
            self._result = results
        return results

    def get_persist_path(self):
        work_dir = self.pipeline.work_dir
        persist_dir = os.path.join(work_dir, self.pipeline.name, 'persist')
        return os.path.join(persist_dir, self.name+".parquet")

    def __copy__(self):
        inputs = [copy(node) for node in self.inputs]
        return DaskDataFrameOperation(name=self.name,
                                      pipeline=None,
                                      operation=self.operation,
                                      inputs=inputs,
                                      is_output=self.is_output,
                                      persist=self.persist,
                                      keep=self.keep)
