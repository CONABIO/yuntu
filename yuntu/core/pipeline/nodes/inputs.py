"""Input pipeline nodes."""
from abc import ABC
from abc import abstractmethod
import os
import pickle
import dill
import numpy as np
import pandas as pd
import dask.dataframe as dd
from yuntu.core.pipeline.nodes.base import Node


class Input(Node, ABC):
    """Input node base class."""

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

    def __copy__(self):
        return PickleableInput(name=self.name,
                               pipeline=None,
                               data=self.data)


class NumpyArrayInput(PickleableInput):

    def validate(self, data):
        if data is None:
            return False
        if not isinstance(data, np.array):
            return False
        return True

    def __copy__(self):
        return NumpyArrayInput(name=self.name,
                               pipeline=None,
                               data=self.data)


class DictInput(PickleableInput):

    def validate(self, data):
        return super().validate(data) and isinstance(data, dict)

    def __copy__(self):
        return DictInput(name=self.name,
                         pipeline=None,
                         data=self.data)


class ScalarInput(PickleableInput):

    def validate(self, data):
        if isinstance(data, str):
            return True
        return (super().validate(data) and
                not hasattr(data, '__len__'))

    def __copy__(self):
        return ScalarInput(name=self.name,
                           pipeline=None,
                           data=self.data)


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

    def __copy__(self):
        return PandasDataFrameInput(name=self.name,
                                    pipeline=None,
                                    data=self.data)


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

    def __copy__(self):
        return DaskDataFrameInput(name=self.name,
                                  pipeline=None,
                                  data=self.data)
