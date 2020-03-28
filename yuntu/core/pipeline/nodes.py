"""Computation nodes classes."""
from abc import ABC
from abc import abstractmethod
import os
import pickle
import dill
import numpy as np
import pandas as pd
import dask.dataframe as dd
import functools


class Node(ABC):

    def __init__(self, name=None, pipeline=None):
        if name is not None:
            self.name = name
        else:
            self.name = 'node'
        self.pipeline = pipeline

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline
        self.attach()

    def is_persisted(self):
        return os.path.exists(self.get_persist_path())

    def clear(self):
        if self.is_persisted():
            os.remove(self.get_persist_path())

    @abstractmethod
    def get_persist_path(self):
        """Path to operation persisted outputs."""

    @abstractmethod
    def attach(self):
        """Add self to pipeline."""

    @abstractmethod
    def read(self, path, **kwargs):
        """Read node from path."""

    @abstractmethod
    def write(self, path, **kwargs):
        """Write node to path."""


class Input(Node, ABC):
    def __init__(self, *args, data=None, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.validate(data):
            message = "Data is invalid for this type of node."
            raise ValueError(message)
        self.result = data
        if self.pipeline is not None:
            self.attach()

    @property
    def data(self):
        return self.result

    @abstractmethod
    def validate(self, data):
        """Validate data."""

    def attach(self):
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        self.pipeline.add_input(self.name,
                                self.data)
        self.pipeline.nodes[self.name] = self


class PickleableInput(Input):

    def validate(self, data):
        return dill.pickles(data)

    def write(self, path=None, data=None):
        if path is None:
            path = self.get_persist_path()
        if data is None:
            data = self.data
        if not self.validate(data):
            message = "Data is invalid."
            raise ValueError(message)
        with open(path, 'wb') as file:
            pickle.dump(data, file)

    def read(self, path=None):
        if path is None:
            path = self.get_persist_path()
        if not os.path.exists(path):
            message = "No pickled data at path."
            raise ValueError(message)
        with open(path) as file:
            data = pickle.load(file)
        return data

    def get_persist_path(self):
        work_dir = self.pipeline.work_dir
        persist_dir = os.path.join(work_dir, self.pipeline.name, 'persist')
        return os.path.join(persist_dir, self.name+".pickle")


class NumpyArrayInput(PickleableInput):

    def validate(self, data):
        if data is None:
            return False
        if not isinstance(data, np.array):
            return False
        return True


class DictInput(PickleableInput):

    def validate(self, data):
        return super().validate(data) and isinstance(data, dict)


class ScalarInput(PickleableInput):

    def validate(self, data):
        if isinstance(data, str):
            return True
        return (super().validate(data) and
                not hasattr(data, '__len__'))


class PandasDataFrameInput(PickleableInput):

    def validate(self, data):
        return isinstance(data, pd.DataFrame)

    def write(self, path=None, data=None):
        if path is None:
            path = self.get_persist_path()
        if data is None:
            data = self.data
        if not self.validate(data):
            message = "Data is invalid."
            raise ValueError(message)
        data.to_pickle(path, None)

    def read(self, path=None):
        if path is None:
            path = self.get_persist_path()
        if not os.path.exists(path):
            message = "No pickled data at path."
            raise ValueError(message)
        data = pd.read_pickle(path, None)
        return data


class DaskDataFrameInput(Input):

    def validate(self, data):
        return isinstance(data, dd)

    def write(self, path=None, data=None):
        if path is None:
            path = self.get_persist_path()
        if data is None:
            data = self.data
        if not self.validate(data):
            message = "Data is invalid."
            raise ValueError(message)
        return data.to_parquet(self.get_persist_path(), compression="GZIP")

    def read(self, path=None):
        if path is None:
            path = self.get_persist_path()
        if not os.path.exists(path):
            message = "No persisted data at path."
            raise ValueError(message)
        return dd.read_parquet(self.get_persist_path())

    def get_persist_path(self):
        work_dir = self.pipeline.work_dir
        persist_dir = os.path.join(work_dir, 'persist')
        return os.path.join(persist_dir, self.name+".parquet")


class Operation(Node, ABC):

    def __init__(self,
                 *args,
                 operation=None,
                 inputs=None,
                 is_output=False,
                 persist=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation = operation
        self.inputs = inputs
        self.is_output = is_output
        self.persist = persist
        self.result = None
        if self.pipeline is not None and self.operation is not None:
            self.attach()

    def compute(self, force=False, **kwargs):
        """Compute self."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        self.result = self.pipeline.compute(nodes=[self.name],
                                            force=force,
                                            **kwargs)
        return self.result

    def attach(self):
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'set_pipeline'."
            raise ValueError(message)
        self.pipeline.add_operation(self.name,
                                    self.operation,
                                    self.inputs,
                                    self.is_output,
                                    self.persist)
        self.pipeline.nodes[self.name] = self

    @abstractmethod
    def __call__(self, func):
        """Method to use class as decorator"""


class DaskOperation(Operation, ABC):

    def compute(self, force=False, client=None):
        if not force and self.result is not None:
            return self.result
        self.result = self.pipeline.compute([self.name],
                                            force=force,
                                            client=client)[self.name]
        return self.result


class DaskDataFrameOperation(DaskOperation):

    def write(self, path=None, dataframe=None):
        if path is None:
            path = self.get_persist_path()
        if dataframe is not None:
            if not isinstance(dataframe, dd):
                message = "Argument 'dataframe' must be a dask dataframe."
                raise ValueError(message)
            results = dataframe
        elif self.result is not None:
            results = self.result
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
        return dd.read_parquet(self.get_persist_path())

    def get_persist_path(self):
        work_dir = self.pipeline.work_dir
        persist_dir = os.path.join(work_dir, self.pipeline.name, 'persist')
        return os.path.join(persist_dir, self.name+".parquet")

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            all_args = list(args) + [kwargs[key] for key in kwargs]
            pipeline = self.pipeline
            if pipeline is None:
                if len(all_args) == 0:
                    raise ValueError("No pipeline.")
                pipeline = all_args[0].pipeline
            for arg in all_args:
                if arg.pipeline != pipeline:
                    raise ValueError('Nodes have different pipelines.')
            inputs = [arg.name for arg in all_args]
            self.inputs = inputs
            self.operation = func
            self.set_pipeline(pipeline)
            return self
        return wrapper
