"""Base class for dask pipeline."""
import os
from collections import OrderedDict
import copy
from dask.threaded import get
import dask.dataframe as dd
from dask.optimization import cull, inline, inline_functions, fuse
from yuntu.core.pipeline.base import Pipeline
from yuntu.core.pipeline.nodes.inputs import DictInput, Input
from yuntu.core.pipeline.nodes.base import Node

DASK_CONFIG = {'npartitions': 1}


def linearize_operations(op_names, graph):
    """Linearize dask operations."""
    graph1, deps = cull(graph, op_names)
    graph2 = inline(graph1, dependencies=deps)
    graph3 = inline_functions(graph2,
                              op_names,
                              [len, str.split],
                              dependencies=deps)
    graph4, deps = fuse(graph3)
    return graph4


class DaskPipeline(Pipeline):
    """Pipeline that uses dask as a parallel processing manager."""

    def __init__(self,
                 *args,
                 work_dir=None,
                 **kwargs):
        super().__init__(*args, *kwargs)
        if work_dir is None:
            work_dir = "/tmp"
        if not os.path.isdir(work_dir):
            message = "Argument 'work_dir' must be a valid directory."
            raise ValueError(message)
        self.work_dir = work_dir
        self.graph = {}
        self.init_pipeline()

    def init_pipeline(self):
        """Initialize pipeline."""
        base_dir = os.path.join(self.work_dir, self.name)
        persist_dir = os.path.join(base_dir, 'persist')
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)
        if not os.path.exists(persist_dir):
            os.mkdir(persist_dir)

    def build(self):
        """Add operations that are specific to each pipeline."""

    def _add_operation(self,
                       name,
                       operation,
                       inputs=None):
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

    def _add_input(self, name, data):
        """Add input."""
        if not isinstance(name, str):
            message = "Argument 'name' must be a string."
            raise ValueError(message)
        if data is None:
            message = "Argument 'data' can not be undefined."
            raise ValueError(message)
        self.graph[name] = data

    def remove_node(self, node):
        """Remove node."""
        if not isinstance(node, Node):
            if isinstance(node, str):
                self.nodes[node].clear()
                del self.nodes[node]
                del self.graph[node]
            else:
                raise TypeError("Argument must be a node or a string.")
        else:
            name = node.name
            if self.node_exists(name):
                self.nodes[name].clear()
                del self.nodes[name]
                del self.graph[name]
            else:
                raise KeyError(f"Node {name} does not exist.")

    def write_node(self, name, result):
        """Persist computations as files."""
        if self.node_exists(name):
            return self.nodes[name].write()
        raise ValueError("Node does not exist.")

    def read_node(self, name):
        """Read persisted node."""
        if self.node_exists(name):
            return self.nodes[name].read()
        raise ValueError("Node does not exist.")

    def clear_persisted(self, name):
        """Delete persisted node."""
        if self.node_exists(name):
            return self.nodes[name].clear()
        raise ValueError("Node does not exist.")

    def clear(self):
        """Clear all persisted information."""
        for name in self.nodes:
            self.nodes[name].clear()

    def get_nodes(self,
                  names,
                  client=None,
                  compute=False,
                  force=False,
                  linearize=None):
        for name in names:
            if not self.node_exists(name):
                raise ValueError('No node named '+name)
        graph = dict(self.graph)
        if linearize is not None:
            if not isinstance(linearize, (tuple, list)):
                message = ("Argument 'linearize' must be a tuple or a list " +
                           "of node names.")
                raise ValueError(message)
            for name in linearize:
                if not isinstance(name, str):
                    message = ("Node names within 'linearize' must be " +
                               "strings.")
                    raise KeyError(message)
                if name not in self.nodes:
                    message = (f"No node named {name} within this pipeline")
            graph = linearize_operations(linearize, graph)

        results = {}
        to_retrieve = []
        for name in names:
            if isinstance(self.nodes[name], Input):
                results[name] = self.nodes[name].data
            elif self.nodes[name].persist:
                if not force and self.nodes[name].is_persisted():
                    node = self.nodes[name].read()
                    if compute and hasattr(node, 'compute'):
                        node = node.compute()
                    if self.nodes[name].keep:
                        self.nodes[name].result = node
                    results[name] = node
                else:
                    to_retrieve.append(name)
            else:
                to_retrieve.append(name)
        if client is not None:
            retrieved = client.get(graph,
                                   to_retrieve,
                                   sync=False).result()
        else:
            retrieved = get(graph, to_retrieve)
        for i in range(len(to_retrieve)):
            node = retrieved[i]
            if compute and hasattr(node, 'compute'):
                node = node.compute()
                if self.nodes[name].persist:
                    self.nodes[name].write(data=node)
                if self.nodes[name].keep:
                    self.nodes[name].result = node
            results[name] = node
        return results

    def get_node(self,
                 name,
                 client=None,
                 compute=False,
                 force=False):
        """Get node from pipeline graph."""
        return self.get_nodes(names=[name],
                              client=client,
                              compute=compute,
                              force=force,
                              linearize=[name])[name]

    def compute(self,
                nodes=None,
                force=False,
                client=None,
                dask_config=None,
                linearize=None):
        """Compute pipeline."""
        if dask_config is None:
            dask_config = DASK_CONFIG
        DictInput("dask_config",
                  data=dask_config,
                  pipeline=self)
        if nodes is None:
            nodes = [name for name in self.nodes if self.nodes[name].is_output]
        if not isinstance(nodes, (tuple, list)):
            message = "Argument 'nodes' must be a tuple or a list."
            raise ValueError(message)
        return self.get_nodes(nodes,
                              client=client,
                              compute=True,
                              force=force,
                              linearize=linearize)

    def __and__(self, other):
        """Disjoint parallelism operator '&'.

        Returns a new pipeline with nodes from both pipelines running in
        disjoint paralellism. If node names are duplicated, subindices '0' and
        '1' are employed to indicate the origin (left and right). The resulting
        pipeline's 'work_dir' will be that of the first operand.
        """
        name = self.name + "&" + other.name
        work_dir = self.work_dir
        new_pipeline = DaskPipeline(name, work_dir=work_dir)
        node_rel = {}
        for key in self.nodes:
            if key not in node_rel:
                node_rel[key] = []
            node_rel[key].append(self.nodes[key])
        for key in other.nodes:
            if key not in node_rel:
                node_rel[key] = []
            node_rel[key].append(other.nodes[key])
        for key in node_rel:
            if len(node_rel[key]) > 1 and key != "dask_config":
                for i in range(len(node_rel[key])):
                    to_add = copy.copy(node_rel[key][i])
                    new_name = f"{to_add.name}_{i}"
                    new_inputs = []
                    if hasattr(to_add, 'inputs'):
                        for inp in range(len(to_add.inputs)):
                            input_name = to_add.inputs[inp]
                            if (len(node_rel[input_name]) > 1 and
                               input_name != "dask_config"):
                                input_name = f"{input_name}_{i}"
                            new_inputs.append(input_name)
                        to_add.inputs = new_inputs
                    to_add.name = new_name
                    new_pipeline.add_node(to_add)
            else:
                new_pipeline.add_node(copy.copy(node_rel[key][0]))
        return new_pipeline

    def knit_inputs(self, knit_map):
        """Reduce input nodes by mapping and return new pipeline."""
