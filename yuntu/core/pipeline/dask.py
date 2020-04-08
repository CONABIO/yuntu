"""Base class for dask pipeline."""
import os
from collections import OrderedDict
from copy import copy
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

    @property
    def graph(self):
        if len(self.nodes) == 0:
            return None
        graph = {}
        for key in self.keys():
            node = self.nodes[key]
            if key not in self.deps:
                message = (f"Dependencies for node {key} have not been" +
                           "assigned yet.")
                raise KeyError(message)
            if self.deps[key] is None:
                graph[key] = node.data
            else:
                inputs = []
                for dkey in self.deps[key]:
                    if dkey not in self.nodes:
                        raise KeyError(f"Dependency for node {key} with " +
                                       f"key {dkey} does not exist.")
                    inputs.append(dkey)
                if len(inputs) > 0:
                    graph[key] = (node.operation, *inputs)
                else:
                    graph[key] = (node.operation,)
        return graph

    def clear(self):
        """Clear all persisted information."""
        for name in self.nodes:
            self.nodes[name].clear()

    def get_nodes(self,
                  nodes,
                  compute=False,
                  force=False,
                  client=None,
                  linearize=None):
        if len(nodes) == 0:
            raise ValueError("At least one node must be specified.")
        for key in nodes:
            if key not in self:
                raise ValueError(f'No node with key {key}')
        graph = self.graph
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
        for key in nodes:
            if isinstance(self.nodes[key], Input):
                results[key] = self.nodes[key].data
            elif self.nodes[key].persist:
                if not force and self.nodes[key].is_persisted():
                    node = self.nodes[key].read()
                    if compute and hasattr(node, 'compute'):
                        node = node.compute()
                    if self.nodes[key].keep:
                        self.nodes[key].set_value(node)
                    results[key] = node
                else:
                    to_retrieve.append(key)
            else:
                to_retrieve.append(key)
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
                if self.nodes[key].persist:
                    self.nodes[key].write(data=node)
                if self.nodes[key].keep:
                    self.nodes[key].set_value(node)
            results[key] = node
        return results

    def get_node(self,
                 key,
                 compute=False,
                 force=False,
                 client=None):
        """Get node from pipeline graph."""
        return self.get_nodes(nodes=[key],
                              compute=compute,
                              force=force,
                              client=client,
                              linearize=[key])[key]

    def compute(self,
                nodes=None,
                force=False,
                client=None,
                dask_config=None,
                linearize=None):
        """Compute pipeline."""
        if nodes is None:
            nodes = [key for key in self.nodes if self.nodes[key].is_output]

        if not isinstance(nodes, (tuple, list)):
            message = "Argument 'nodes' must be a tuple or a list."
            raise ValueError(message)

        if dask_config is None:
            dask_config = DASK_CONFIG

        config = DictInput("dask_config", data=dask_config)
        self['dask_config'] = config
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
        dask_config = DictInput("dask_config", data=DASK_CONFIG)
        dask_config.set_pipeline(new_pipeline)
        dask_config.attach()
        node_map = {"self": {"dask_config": dask_config},
                    "other": {"dask_config": dask_config}}
        for operand in ["self", "other"]:
            if operand == "self":
                pipeline = self
            else:
                pipeline = other
            for key in pipeline.operations:
                node = pipeline.nodes[key]
                input_keys = [inp.key for inp in node.inputs]
                new_inputs = []
                for dkey in input_keys:
                    if dkey not in node_map[operand]:
                        dep = pipeline.nodes[dkey]
                        new_dep = copy(dep)
                        new_dep.set_pipeline(new_pipeline)
                        new_dep.attach()
                        node_map[operand][dkey] = new_dep
                        new_inputs.append(new_dep)
                    else:
                        new_inputs.append(node_map[operand][dkey])
                new_node = copy(node)
                new_node.set_inputs(new_inputs)
                new_node.set_pipeline(new_pipeline)
                new_node.attach()
                node_map[operand][key] = new_node
        return new_pipeline

    def knit_inputs(self, knit_map):
        """Reduce input nodes by mapping and return new pipeline."""
