"""Base class for dask pipeline."""
import os
import shutil
from dask.threaded import get
import dask.dataframe as dd
from dask.optimization import cull, inline, inline_functions, fuse
from yuntu.core.pipeline.base import Pipeline


class DaskPipeline(Pipeline):
    """Pipeline that uses dask as a parallel processing manager."""

    def __init__(self,
                 *args,
                 work_dir,
                 client=None,
                 npartitions=10,
                 overwrite=False,
                 **kwargs):
        super().__init__(*args, *kwargs)
        if not os.path.isdir(work_dir):
            message = "Argument 'work_dir' must be a valid directory."
            raise ValueError(message)
        if client is not None:
            if npartitions is None:
                self.npartitions = 1
            else:
                if npartitions <= 0:
                    message = "Wrong number of partitions. Should be > 0."
                    raise ValueError(message)
                self.npartitions = npartitions
        self._linearize = []
        self.work_dir = work_dir
        self.client = client
        self.graph = {}
        self.init_pipeline(overwrite)

    def init_pipeline(self, overwrite=False):
        """Initialize pipeline."""
        base_dir = os.path.join(self.work_dir, self.name)
        persist_dir = os.path.join(base_dir, 'persist')
        if os.path.exists(base_dir):
            if overwrite:
                shutil.rmtree(base_dir)
                os.mkdir(base_dir)
        else:
            os.mkdir(base_dir)
        if not os.path.exists(persist_dir):
            os.mkdir(persist_dir)

    def add_operation(self,
                      name,
                      operation,
                      inputs=None,
                      is_output=False,
                      persist=False,
                      linearize=False):
        """Add operation."""
        if not isinstance(name, str):
            message = "Argument 'name' must be a string."
            raise ValueError(message)
        if not hasattr(operation, '__call__'):
            message = "Argument 'operation' must be a callable object."
            raise ValueError(message)
        if inputs is not None:
            if not isinstance(inputs, (tuple, list)):
                message = "Argument 'inputs' must be a list or a tuple" + \
                          " of node names."
                raise ValueError(message)
            self.graph[name] = (operation, *inputs)
        else:
            self.graph[name] = (operation,)

        if is_output:
            self._mark_output(name)
        elif persist:
            self._mark_persist(name)
        if linearize:
            self._mark_linearize(name)

    def add_input(self, name, data):
        """Add input."""
        if not isinstance(name, str):
            message = "Argument 'name' must be a string."
            raise ValueError(message)
        if data is None:
            message = "Argument 'data' can not be undefined."
            raise ValueError(message)
        self.graph[name] = data

    def write_node(self, name, result):
        """Persist computations as files."""
        path = self._build_persisted_path(name)
        return result.to_parquet(path, compression="GZIP")

    def read_node(self, name):
        """Read persisted node."""
        path = self._build_persisted_path(name)
        return dd.read_parquet(path)

    def clear_persisted(self, name):
        """Delete persisted node."""
        path = self._build_persisted_path(name)
        if os.path.exists(path):
            os.remove(path)

    def clear(self):
        """Clear all persisted information."""
        for name in self.persist:
            self.clear_persisted(name)

    def get_node(self, name, compute=False, force=False):
        """Get node from pipeline graph."""
        if not isinstance(name, str):
            message = "Argument 'name' must be a string."
            raise ValueError(message)

        if name in self.persist:
            persist = True

        if not force and os.path.exists(self._build_persisted_path(name)):
            node = self.read_node(name)
            persist = False
        elif self.client is not None:
            node = self.client.get(self.graph,
                                   name,
                                   sync=False).result()
        else:
            node = get(self.graph, name)

        if compute and self.client is not None:
            result = node.compute()
            if persist:
                self.write_node(name, result)
            return result
        return node

    def compute(self, nodes=None, force=False):
        """Compute pipeline."""
        results = {}
        if nodes is None:
            nodes = self.outputs
        if not isinstance(nodes, (tuple, list)):
            message = "Argument 'nodes' must be a tuple or a list."
            raise ValueError(message)
        for name in nodes:
            results[name] = self.get_node(name, compute=True, force=force)
        return results

    def linearize_operations(self, operations=None):
        """Linearize dask operations."""
        if operations is not None:
            if not isinstance(operations, (tuple, list)):
                message = "Argument 'operations' should be a tuple or a " + \
                          "list of node names."
                raise ValueError(message)
        else:
            operations = self._linearize
        graph1, deps = cull(self.graph, operations)
        graph2 = inline(graph1, dependencies=deps)
        graph3 = inline_functions(graph2,
                                  operations,
                                  [len, str.split],
                                  dependencies=deps)
        graph4, deps = fuse(graph3)
        self.graph = graph4

    def _build_persisted_path(self, name):
        """Return path to persisted directory within 'work_dir'."""
        return os.path.join(self.work_dir,
                            self.name,
                            "persist",
                            name+".parquet")

    def _mark_linearize(self, name):
        """Mark operation to be linearized on computation."""
        if name not in self._linearize:
            self._linearize.append(name)
