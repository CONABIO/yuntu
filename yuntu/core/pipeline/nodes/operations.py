"""Operation pipeline nodes."""
from abc import ABC
from abc import abstractmethod
import os
from yuntu.core.pipeline.nodes.base import Node
import dask.dataframe as dd
import pandas as pd


class Operation(Node, ABC):
    """Pipeline operation base node."""
    def __init__(self,
                 *args,
                 operation=None,
                 inputs=None,
                 is_output=False,
                 persist=False,
                 keep=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation = operation
        self.inputs = inputs
        self.is_output = is_output
        self.persist = persist
        self.keep = keep
        self.result = None
        if self.pipeline is not None and self.operation is not None:
            self.attach()

    @abstractmethod
    def compute(self, force=False, client=None, dask_config=None):
        """Compute self."""


class DaskOperation(Operation, ABC):
    def compute(self, force=False, client=None, dask_config=None):
        """Compute self."""
        if self.pipeline is None:
            message = "This node does not belong to any pipeline. Please " + \
                      " assign a pipeline using method 'attach'."
            raise ValueError(message)
        if not force and self.result is not None:
            return self.result
        result = self.pipeline.compute([self.name],
                                       force=force,
                                       client=None,
                                       dask_config=dask_config)[self.name]
        if self.keep:
            self.result = result
        return result


class DaskDataFrameOperation(DaskOperation):

    def write(self, path=None, data=None):
        if path is None:
            path = self.get_persist_path()
        if data is not None:
            if not isinstance(data, (pd.DataFrame,
                                     dd.core.DataFrame)):
                message = "Argument 'data' must be a dask or pandas dataframe."
                raise ValueError(message)
            results = data
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
        results = dd.read_parquet(self.get_persist_path())
        if self.keep:
            self.results = results
        return results

    def get_persist_path(self):
        work_dir = self.pipeline.work_dir
        persist_dir = os.path.join(work_dir, self.pipeline.name, 'persist')
        return os.path.join(persist_dir, self.name+".parquet")

    def __copy__(self):
        return DaskDataFrameOperation(name=self.name,
                                      pipeline=None,
                                      operation=self.operation,
                                      inputs=self.inputs,
                                      is_output=self.is_output,
                                      persist=self.persist,
                                      keep=self.keep)
