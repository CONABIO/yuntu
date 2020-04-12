"""Base class for dask pipeline."""

import os
from copy import copy
import warnings
import networkx as nx
import dask.bag as dask_bag
from dask.threaded import get
from dask.optimization import cull
from dask.optimization import inline as dask_inline
from dask.optimization import inline_functions
from dask.optimization import fuse
from yuntu.core.pipeline.base import Pipeline

DASK_CONFIG = {'npartitions': 1}


def linearize_operations(op_names, graph):
    """Linearize dask operations."""
    graph1, deps = cull(graph, op_names)
    graph2 = dask_inline(graph1, dependencies=deps)
    graph3 = inline_functions(graph2,
                              op_names,
                              [len, str.split],
                              dependencies=deps)
    graph4, deps = fuse(graph3)
    return graph4


def project_output(output, index):
    if index is not None:
        return output[index]
    return output


def union(p1, p2, new=True):
    if not new:
        new_pipeline = p1
    else:
        name = f"union({p1.name},{p2.name})"
        new_pipeline = copy(p1)
        new_pipeline.name = name

    node_map = {}
    for key in p2.inputs:
        node = p1.nodes[key]
        new_node = copy(node)
        new_pipeline.add_place(new_node)
        node_map[key] = new_node

    for key in p2.transitions:
        if key not in node_map:
            node = p2.nodes[key]
            new_node = copy(node)
            new_node.set_pipeline(new_pipeline)
            new_inputs = []
            new_outputs = []

            for ind, inode in enumerate(node.inputs):
                ikey = inode.key
                if ikey not in node_map:
                    new_in = copy(inode)
                    new_in.set_pipeline(new_pipeline)
                    node_map[ikey] = new_in
                else:
                    new_in = node_map[ikey]
                new_inputs.append(new_in)

            for ind, onode in enumerate(node.outputs):
                okey = onode.key
                if okey not in node_map:
                    new_out = copy(onode)
                    new_out.set_pipeline(new_pipeline)
                    node_map[okey] = new_out
                else:
                    new_out = node_map[okey]
                new_outputs.append(new_out)

            new_node.set_inputs(new_inputs)
            new_node.set_outputs(new_outputs)

            new_node.attach()
            node_map[key] = new_node

    return new_pipeline


def merge(p1, p2, on='key', knit_points=None, new=True, prune=True):
    if on is None:
        on = "key"

    if knit_points is None:
        if on not in ["key", "name"]:
            raise ValueError("Argument 'by' should be 'key' or 'name'.")

        knit_points = {}
        if on == "key":
            common_keys = []
            for p1_key in p1.nodes:
                for p2_key in p2.nodes:
                    if (p1_key == p2_key
                       and p1.nodes[p1_key].is_compatible(p2.nodes[p2_key])
                       and p1_key not in common_keys):
                        common_keys.append(p1_key)
            knit_points = {}
            for key in common_keys:
                knit_points[key] = {'map': key,
                                    'new_key': key,
                                    'new_name': p1.nodes[key].name}
        else:
            if (len(p1.keys()) != len(p1.names) or
               len(p2.keys() != len(p2.names))):
                message = ("Names are duplicated within one of the " +
                           "pipelines. Change names, prune pipelines " +
                           "or use on='key'")
                raise ValueError(message)

            common_names = list(set(list(p1.names)) &
                                set(list(p2.names)))
            for name in common_names:
                p2_key = None
                for key in p2.nodes:
                    if p2.nodes[key].name == name:
                        p2_key = key
                        break

                p1_key = None
                for key in p1.nodes:
                    if p1.nodes[key].name == name:
                        p1_key = key
                        break

                if (p1.nodes[p1_key].is_compatible(p2.nodes[p2_key])
                   and p2_key not in knit_points):
                    knit_points[p2_key] = {"map": p1_key,
                                           "new_key": p1_key,
                                           "new_name": name}
    else:
        knit_keys = list(knit_points)
        for key in knit_keys:
            p1_key = knit_points[key]["map"]
            new_key = knit_points[key]["new_key"]
            safe = True

            if new_key in p1.keys():
                if new_key != p1_key:
                    safe = False
            if new_key in p2.keys():
                if new_key != key:
                    safe = False

            if not safe:
                message = ("Attribute 'new_key' of knit points " +
                           "should be a brand new key within both " +
                           "pipelines, otherwise entry key,  " +
                           "'new_key' and 'map' should be equal.")
                raise ValueError(message)

            if key not in p2.keys():
                message = (f"Key {key} not found in second pipeline. " +
                           "All keys in argument " +
                           "'knit_points' must exist within the second " +
                           "pipeline. Removing entry from" +
                           " knitting points.")
                warnings.warn(message)

            if p1_key not in p1.keys():
                message = (f"Key {p1_key} not found " +
                           "in first pipeline. All map keys in argument " +
                           "'knit_points' must exist within the first " +
                           "pipeline. Removing entry from" +
                           " knitting points.")
                warnings.warn(message)

            if not p1.nodes[p1_key].is_compatible(p2.nodes[key]):
                message = (f"Node {p1_key} of first pipeline is not " +
                           f"compatible with node {key} of second " +
                           "pipeline. Removing entry from" +
                           " knitting points.")
                warnings.warn(message)

    if not new:
        new_pipeline = p1
    else:
        name = f"merge({p1.name},{p2.name})"
        new_pipeline = copy(p1)
        new_pipeline.name = name

    knit_points_inv = {}
    for key in knit_points:
        new_key = knit_points[key]["new_key"]
        new_name = knit_points[key]["new_name"]
        p1_key = knit_points[key]["map"]
        new_pipeline[new_key] = new_pipeline[p1_key]
        new_pipeline[new_key].name = new_name
        knit_points_inv[p1_key] = {"new_key": new_key,
                                   "map": key,
                                   "new_name": new_name}

    node_map = {}
    orders = p2.node_order()
    all_orders = list(set(orders[ordkey]
                          for ordkey in orders))
    all_orders.sort()
    for level in all_orders:
        level_keys = [key for key in p2.nodes
                      if orders[key] == level]
        for key in level_keys:
            if key not in node_map:
                if key in knit_points:
                    new_key = knit_points[key]["new_key"]
                else:
                    new_key = key
                if new_key not in new_pipeline:
                    node = p2.nodes[key]
                    if key in p2.inputs:
                        new_node = copy(node)
                        new_node.set_pipeline(new_pipeline)
                        new_node.attach()
                        node_map[key] = new_node
                    elif key in p2.transitions:
                        new_node = copy(node)
                        new_node.set_pipeline(new_pipeline)
                        new_inputs = []
                        new_outputs = []

                        for ind, inode in enumerate(node.inputs):
                            ikey = inode.key
                            if ikey in knit_points:
                                new_ikey = knit_points[ikey]["new_key"]
                            else:
                                new_ikey = ikey
                            if new_ikey not in new_pipeline:
                                if ikey not in node_map:
                                    new_in = copy(inode)
                                    new_in.set_pipeline(new_pipeline)
                                    node_map[ikey] = new_in
                                else:
                                    new_in = node_map[ikey]
                            new_inputs.append(new_in)

                        for ind, onode in enumerate(node.outputs):
                            okey = onode.key
                            if okey in knit_points:
                                new_okey = knit_points[okey]["new_key"]
                            else:
                                new_okey = okey
                            if new_okey not in new_pipeline:
                                if okey not in node_map:
                                    new_out = copy(onode)
                                    new_out.set_pipeline(new_pipeline)
                                    node_map[okey] = new_out
                                else:
                                    new_out = node_map[okey]
                            new_outputs.append(new_out)

                        new_node.set_inputs(new_inputs)
                        new_node.set_outputs(new_outputs)

                        for ind, inode in enumerate(node.inputs):
                            ikey = inode.key
                            if new_node.inputs[ind] not in new_pipeline:
                                new_pipeline[ikey] = new_node.inputs[ind]

                        for ind, onode in enumerate(node.outputs):
                            okey = onode.key
                            if new_node.outputs[ind] not in new_pipeline:
                                new_pipeline[okey] = new_node.outputs[ind]

                        new_pipeline[new_key] = new_node
                        node_map[key] = new_node
                else:
                    node_map[key] = new_pipeline[new_key]

    if prune:
        new_pipeline.prune()

    return new_pipeline


class DaskPipeline(Pipeline):
    """Pipeline that uses dask as a parallel processing manager."""

    def __init__(self,
                 name,
                 work_dir=None,
                 **kwargs):
        super().__init__(name)
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

    def build_graph(self, feed=None, linearize=None):
        """Build a dask computing graph."""
        if len(self.nodes) == 0:
            raise ValueError("Can not buid a graph from an empty pipeline.")

        if feed is not None:
            if not isinstance(feed, dict):
                message = ("Feed argument must be a dictionary with node" +
                           " names as arguments and input data as values.")
                raise ValueError(message)

        graph = {}
        if feed is not None:
            for key in feed:
                node = self.nodes[key]
                if not node.validate(feed[key]):
                    raise ValueError("Feeding data is invalid for node " +
                                     f"{key}")
                graph[key] = feed[key]

        for key in self.keys():
            node = self.nodes[key]
            if key not in self.nodes_down or key not in self.nodes_up:
                message = (f"Neighbours for node {key} have not been" +
                           " assigned yet.")
                raise KeyError(message)
            if key in self.places:
                if key in self.inputs:
                    if key not in graph:
                        graph[key] = node.data
            else:
                if key not in graph:
                    inputs = []
                    for dkey in self.nodes_up[key]:
                        inputs.append(dkey)
                    graph[key] = (node.operation, *inputs)

                down_nodes = self.nodes_down[key]
                n_down = len(down_nodes)
                for i in range(n_down):
                    dkey = down_nodes[i]
                    if dkey not in graph:
                        index_node = f"{dkey}_index"
                        if n_down == 1:
                            graph[index_node] = None
                        else:
                            graph[index_node] = i
                        dargs = (key, index_node)
                        graph[dkey] = (project_output, *dargs)

        if linearize is not None:
            if not isinstance(linearize, (tuple, list)):
                message = ("Argument 'linearize' must be a tuple or a list " +
                           "of node names.")
                raise ValueError(message)
            for key in linearize:
                if not isinstance(key, str):
                    message = ("Node names within 'linearize' must be " +
                               "strings.")
                    raise KeyError(message)
                if key not in self.nodes:
                    message = (f"No node named {key} within this pipeline")
            graph = linearize_operations(linearize, graph)

        return graph

    @property
    def graph(self):
        """Returns full dask computation graph."""
        return self.build_graph()

    def clear(self):
        """Clear all persisted information."""
        for name in self.nodes:
            self.nodes[name].clear()

    def get_nodes(self,
                  nodes,
                  feed=None,
                  read=None,
                  write=None,
                  keep=None,
                  compute=False,
                  force=False,
                  client=None,
                  linearize=None):
        if len(nodes) == 0:
            raise ValueError("At least one node must be specified.")

        for key in nodes:
            if key not in self.nodes:
                raise ValueError(f'No node with key {key}')

        if feed is not None:
            if not isinstance(feed, dict):
                message = ("Argument 'feed' must be a dictionary of node " +
                           "keys")
                raise ValueError(message)
            for key in feed:
                if key not in self.nodes:
                    message = f"Key {key} from feed dict not found in nodes."
                    raise KeyError(message)
        else:
            feed = {}

        if read is not None:
            if not isinstance(read, dict):
                message = ("Argument 'read' must be a dictionary of node " +
                           "keys")
                raise ValueError(message)
            for key in read:
                if key not in self.nodes:
                    message = f"Key {key} from read dict not found in nodes."
                    raise KeyError(message)
                if isinstance(read[key], str):
                    path = read[key]
                    if not os.path.exists(read[key]):
                        message = f"Path {path} does not exist"
                        raise ValueError(message)
        else:
            read = {}

        if keep is not None:
            if not isinstance(keep, dict):
                message = ("Argument 'keep' must be a dictionary of node " +
                           "keys")
                raise ValueError(message)
            for key in keep:
                if key not in self.nodes:
                    message = f"Key {key} from keep dict not found in nodes."
                    raise KeyError(message)
        else:
            keep = {}

        if write is not None:
            if not isinstance(keep, dict):
                message = ("Argument 'write' must be a dictionary of node " +
                           "keys")
                raise ValueError(message)
            for key in write:
                if key not in self.nodes:
                    message = f"Key {key} from write dict not found in nodes."
                    raise KeyError(message)
                if not self.nodes[key].can_persist:
                    message = (f"Node {key} is a dynamic node and can not be" +
                               " persisted automatically. You can retrieve " +
                               " the result or future and save them manually.")
                    raise ValueError(message)

        else:
            write = {}

        nxgraph = self.struct

        feed_keys = list(feed.keys())
        for key in feed_keys:
            for nkey in nodes:
                if nkey != key:
                    if self.shortest_path(nkey, key, nxgraph) is not None:
                        del feed[key]

        for key in self.places:
            if key not in read:
                if self.places[key].is_persisted() and not force:
                    read[key] = True

        read_keys = list(read.keys())
        for key in read_keys:
            for nkey in nodes:
                if self.shortest_path(nkey, key, nxgraph) is not None:
                    del read[key]
            for fkey in feed:
                if self.shortest_path(fkey, key, nxgraph) is not None:
                    del read[key]
            for rkey in read_keys:
                if self.shortest_path(rkey, key, nxgraph) is not None:
                    del read[key]

        for key in nodes:
            if key not in keep:
                keep[key] = self.nodes[key].keep

        write_keys = list(write.keys())
        for key in write_keys:
            if key not in nodes:
                del write[key]

        for key in nodes:
            if key in self.places and key not in write:
                if self.nodes[key].can_persist:
                    write[key] = self.nodes[key].persist

        for key in read:
            if isinstance(read[key], bool):
                if read[key]:
                    node = self.nodes[key].read()
                    feed[key] = node
            else:
                node = self.nodes[key].read(path=read[key])
                feed[key] = node

        graph = self.build_graph(feed=feed, linearize=linearize)

        results = {}
        if client is not None:
            retrieved = client.get(graph,
                                   nodes,
                                   sync=False).result()
        else:
            retrieved = get(graph, nodes)

        for ind, xnode in enumerate(retrieved):
            key = nodes[ind]
            if compute and hasattr(xnode, 'compute'):
                node = xnode.compute()
                if isinstance(xnode, dask_bag.core.Bag):
                    data = xnode
                else:
                    data = node
                if key in self.places:
                    if key in write:
                        if isinstance(write[key], str):
                            path = write[key]
                            self.nodes[key].write(path=path, data=data)
                        elif write[key]:
                            self.nodes[key].write(data=data)
                if key in keep:
                    if keep[key]:
                        self.nodes[key].set_value(data)
                results[key] = node
            elif key in self.transitions and self.nodes[key].coarity > 0:
                if compute:
                    computed = []
                    for oind in range(self.nodes[key].coarity):
                        sub_node = xnode[oind]
                        if hasattr(sub_node, "compute"):
                            computed.append(sub_node.compute())
                        else:
                            computed.append(sub_node)
                    results[key] = tuple(computed)
                else:
                    results[key] = xnode
            else:
                results[key] = xnode

        return results

    def get_node(self,
                 key,
                 feed=None,
                 read=None,
                 write=None,
                 keep=None,
                 compute=False,
                 force=False,
                 client=None,
                 linearize=None):
        """Get node from pipeline graph."""
        return self.get_nodes(nodes=[key],
                              feed=feed,
                              read=read,
                              write=write,
                              keep=keep,
                              compute=compute,
                              force=force,
                              client=client,
                              linearize=linearize)[key]

    def compute(self,
                nodes=None,
                feed=None,
                read=None,
                write=None,
                keep=None,
                force=False,
                client=None,
                linearize=None):
        """Compute pipeline."""
        if nodes is None:
            nodes = self.outputs

        if not isinstance(nodes, (tuple, list)):
            message = "Argument 'nodes' must be a tuple or a list."
            raise ValueError(message)

        return self.get_nodes(nodes,
                              feed=feed,
                              read=read,
                              write=write,
                              keep=keep,
                              client=client,
                              compute=True,
                              force=force,
                              linearize=linearize)

    def prune(self):
        """Remove all nodes without any neighbours."""
        G = self.struct
        keys = list(self.keys())
        for key in keys:
            ancestors = [node for node in
                         nx.algorithms.dag.ancestors(G, key)]
            descendants = [node for node in
                           nx.algorithms.dag.descendants(G, key)]
            if len(descendants) == 0 and len(ancestors) == 0:
                del self[key]

    def union(self, other):
        """Disjoint parallel union of self and other."""
        _ = union(self, other, new=False)

    def merge(self,
              other,
              on="key",
              knit_points=None,
              prune=False):
        """Indetify nodes from different pipelines and build a new pipeline.

        Use node keys or names to identify nodes between pipelines directly
        or specify key to key mappings in argument 'knit_points'.
        Ambiguous specifications such as key dupicates for new keys in any
        pipeline as well as incompatibilities between nodes will throw
        exceptions. The resulting pipeline's 'work_dir' will be that of the
        first operand.

        Parameters
        ----------
        other: Pipeline
            Pipeline to knit with.
        on: str
            Specify either 'key' or 'name' to define knitting points in case
            of identification.
        knit_points: dict
            A dictionary that defines key mapping between pipelines and new
            keys and names to be set with attributes of the form:
            <second_pipeline_key>: {
                'map': <first_pipeline_key>,
                'new_key': <key_in_resulting_pipeline>,
                'new_name': <name_in_resulting_pipeline>
            }

        Returns
        -------
        pipeline: Piepeline
            New pipeline with identified nodes.

        Raises
        ------
        ValueError
            If argument 'on' is not 'key' or 'name' or if names are duplicated
            within any of the pipelines.
        KeyError
            If one of the mapping keys does not exist in any of the pipelines.
        """
        _ = merge(self, other,
                  on=on,
                  knit_points=knit_points,
                  prune=prune,
                  new=False)

    def __copy__(self):
        """Return a shallow copy of self."""
        name = f"copy({self.name})"
        work_dir = self.work_dir
        new_pipeline = DaskPipeline(name, work_dir=work_dir)

        for key in self.inputs:
            new_pipeline[key] = copy(self.nodes[key])

        for key in self.transitions:
            if key not in new_pipeline:
                node = self.nodes[key]
                new_node = copy(node)
                new_node.set_pipeline(new_pipeline)
                new_inputs = []
                new_outputs = []

                for ind, inode in enumerate(node.inputs):
                    ikey = inode.key
                    if ikey not in new_pipeline:
                        new_in = copy(inode)
                        new_in.set_pipeline(new_pipeline)
                    else:
                        new_in = new_pipeline[ikey]
                    new_inputs.append(new_in)

                for ind, onode in enumerate(node.outputs):
                    okey = onode.key
                    if okey not in new_pipeline:
                        new_out = copy(onode)
                        new_out.set_pipeline(new_pipeline)
                    else:
                        new_out = new_pipeline[okey]
                    new_outputs.append(new_out)

                new_node.set_inputs(new_inputs)
                new_node.set_outputs(new_outputs)

                for ind, inode in enumerate(node.inputs):
                    ikey = inode.key
                    new_pipeline[ikey] = new_node.inputs[ind]

                for ind, onode in enumerate(node.outputs):
                    okey = onode.key
                    new_pipeline[okey] = new_node.outputs[ind]

                new_pipeline[key] = new_node

        return new_pipeline

    def __and__(self, other):
        """Intetrwine operator '&'.

        Returns a new pipeline that is the result of replacing nodes in the
        second pipeline with nodes from the first pipeline by key
        identification. Nodes from the first pipeline keep all their
        dependencies, disconnecting second pipeline's dependencies.
        Orphan nodes are removed at the end of the process. All other nodes are
        preserved. It folows that if 'p' and 'q' are two pipelines that do not
        share any keys then:
            p & q = p | q
        On the other hand, if 'e' is an empty pipeline and 'p' is any pipeline,
        then the former assumption is True. Additionally we have that
            p & e = p
        and
            p | e = p
        but also
            p & p = p
        The last relation does not hold for disjoint parallelism:
            p | p != p
        The resulting pipeline's 'work_dir' will be that of the first operand.
        """
        name = self.name + "&" + other.name
        new_pipeline = merge(self, other)
        new_pipeline.name = name

        return new_pipeline

    def __or__(self, other):
        """Disjoint sum operator '|'.

        Returns a new pipeline with nodes from both pipelines running in
        disjoint paralellism. The resulting pipeline's 'work_dir' will be that
        of the first operand.

        Parameters
        ----------
        other: Pipeline
            Pipeline to add.

        Returns
        -------
        pipeline: Pipeline
            New pipeline with all nodes from both pipelines, possibly renaming
            some of the nodes where duplicate key's are met.
        """
        name = self.name + "|" + other.name

        new_pipeline = union(self, other)
        new_pipeline.name = name

        return new_pipeline
