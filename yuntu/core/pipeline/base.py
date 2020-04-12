"""Base class for audio processing pipelines."""
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
import uuid
import warnings
import matplotlib as mpl
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
import matplotlib.pyplot as plt
from yuntu.core.pipeline.node.base import Node


class Pipeline(ABC):
    """Base class for processing pipelines."""

    def __init__(self, name):
        if name is None:
            message = "A pipeline must have a name."
            raise ValueError(message)
        self.name = name
        self.nodes = OrderedDict()
        self.places = OrderedDict()
        self.transitions = OrderedDict()
        self.nodes_up = OrderedDict()
        self.nodes_down = OrderedDict()

    def add_node(self, node):
        """Add node to pipeline."""
        if node.is_transition:
            self.add_transition(node)
        elif node.is_place:
            self.add_place(node)
        else:
            message = ("Argument 'node' must be an object of class " +
                       "Place or Tansition.")
            raise ValueError(message)

    def add_place(self, place):
        """Add place node to pipeline.

        Add new place to pipeline.

        Parameters
        ----------
        place: Place
            Place to add.

        Raises
        ------
        ValueError
            If place exists or is not of class Place.
        """
        if not isinstance(place, Node):
            raise ValueError("Argument 'place' must be of class Node.")
        if not place.is_place:
            raise ValueError("Argument 'place' must be of class Place.")
        if place in self:
            raise ValueError(f"Node exists.")
        place.set_pipeline(self)
        key = place.name
        if key is not None:
            if key in self.nodes:
                key = f"{key}-{uuid.uuid1()}"
        else:
            key = f"place-{uuid.uuid1()}"

        self.nodes[key] = place
        self.places[key] = place
        self.add_neighbours(place.key)

    def add_transition(self, transition):
        """Add transition node to pipeline.

        Add new transition to pipeline.

        Parameters
        ----------
        place: Place
            Transition to add.

        Raises
        ------
        ValueError
            If transition exists or is not of class Transition.
        """
        if not isinstance(transition, Node):
            raise ValueError("Argument 'transition' must be of class " +
                             "Node.")
        if not transition.is_transition:
            raise ValueError("Argument 'transition' must be of class " +
                             "Transition.")
        if transition in self:
            raise ValueError(f"Node exists.")
        transition.set_pipeline(self)
        key = transition.name
        if key is not None:
            if key in self.nodes:
                key = f"{key}-{uuid.uuid1()}"
        else:
            key = f"transition-{uuid.uuid1()}"

        self.nodes[key] = transition
        self.transitions[key] = transition
        self.add_neighbours(transition.key)

    def add_neighbours(self, key):
        """Add dependencies to pipeline in case they are absent.

        Parameters
        ----------
        key: str
            Node key within pipeline.

        Raises
        ------
        KeyError
            If key does not exist within pipeline.
        """
        if key not in self.nodes:
            message = "Key not found."
            raise KeyError(message)

        nodes_up = []
        nodes_down = []

        if key in self.places:
            parent = self.nodes[key].parent
            if parent is not None:
                if parent.pipeline != self:
                    parent.set_pipeline(self)
                if parent.key is None:
                    parent.attach()
                nodes_up.append(parent.key)
            for tkey in self.transitions:
                if tkey in self.nodes_up:
                    if key in self.nodes_up[tkey]:
                        nodes_down.append(tkey)
        else:
            for dep in self.nodes[key].inputs:
                if dep.key is None:
                    dep.set_pipeline(self)
                    dep.attach()
                else:
                    if dep.key not in self.nodes:
                        dep.set_pipeline(self)
                        dep.attach()
                    elif dep.pipeline != self:
                        dep.set_pipeline(self)
                        dep.attach()
                # if dep.key is None:
                #     message = ("Automatic input assignation was not " +
                #                f"safe for node with name '{dep.name}'." +
                #                " Please use 'node.set_inputs' to explicitly " +
                #                "assign existing places within pipeline.")
                #     warnings.warn(message)
                nodes_up.append(dep.key)

            for child in self.nodes[key].outputs:
                if child.key is None:
                    child.set_parent(self.nodes[key])
                    child.attach()
                else:
                    if child.key not in self.nodes or child.pipeline != self:
                        child.set_parent(self.nodes[key])
                        child.attach()
                    else:
                        child.set_parent(self.nodes[key])
                        if key not in self.nodes_up[child.key]:
                            self.nodes_up[child.key] = [key]

                # if child.key is None:
                #     message = ("Automatic output assignation was not " +
                #                f"safe for node with name '{child.name}'." +
                #                " Please use 'node.set_outputs' to " +
                #                "explicitly assign existing places " +
                #                "within pipeline.")
                #     warnings.warn(message)
                nodes_down.append(child.key)

        self.nodes_up[key] = nodes_up
        self.nodes_down[key] = nodes_down

    def get_parent(self, key):
        """Get parent for place with key."""
        if key not in self.places:
            return None
        for tkey in self.transitions:
            if tkey in self.nodes_down:
                if key in self.nodes_down[tkey]:
                    return self.transitions[tkey]
        return None

    def upper_neighbours(self, key):
        """Return node's upper neighbours.

        Parameters
        ----------
        key: str
            Node key within pipeline.

        Returns
        -------
        upper: list
            Upper neighbours as list of objects of class Node.

        Raises
        ------
        KeyError
            If key does not exist within pipeline's nodes or dependencies.
        """
        if key not in self.nodes:
            message = "Key not found."
            raise KeyError(message)
        if key not in self.nodes_up:
            message = "Upper neighbours have not been assigned."
            raise KeyError(message)

        return [self.nodes[dkey] for dkey in self.nodes_up[key]]

    def lower_neighbours(self, key):
        """Return node's lower neighbours.

        Parameters
        ----------
        key: str
            Node key within pipeline.

        Returns
        -------
        lower: list
            Lower neighbours as list of objects of class Node.

        Raises
        ------
        KeyError
            If key does not exist within pipeline's nodes or dependencies.
        """
        if key not in self.nodes:
            message = "Key not found."
            raise KeyError(message)
        if key not in self.nodes_down:
            message = "Lower neighbours have not been assigned."
            raise KeyError(message)

        return [self.nodes[dkey] for dkey in self.nodes_down[key]]

    def node_key(self, index):
        """Return node key if argument makes sense as index or as key.

        Parameters
        ----------
        index: int, str
            The index or key to search for.

        Returns
        -------
        key: str
            A valid node key within pipeline.

        Raises
        ------
        IndexError
            If pipeline is empty or index is an integer greater than pipeline's
            length.
        KeyError
            If argument 'index' is a string that is not a key within pipeline.
        ValueError
            If index is not an integer nor a string.
        """
        if len(self) == 0:
            raise IndexError("Pipeline is empty.")
        if isinstance(index, str):
            if index not in self.nodes:
                raise KeyError(f"Key {index} does not exist.")
            return index
        if not isinstance(index, int):
            raise ValueError("Index must be an integer or a string.")
        if index >= len(self):
            raise IndexError("Index must be smaller than pipeline length.")
        keys = list(self.nodes.keys())
        return keys[index]

    def node_index(self, key):
        """Return node index if argument makes sense as key or as index.

        Parameters
        ----------
        key: str, int
            The key or index to search for.

        Returns
        -------
        index: int
            A valid index within pipeline.

        Raises
        ------
        IndexError
            If pipeline is empty or key is an integer that is greater than
            pipeline's length.
        ValueError
            If argument is not a string or an integer.
        KeyError
            If key is a string that is not a key within pipeline.
        """
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
        """Return node name.

        Parameters
        ----------
        key: str, int
            A valid node key or index.

        Returns
        -------
        name: str
            Node name.

        Raises
        ------
        KeyError
            If key/index does not exist or pipeline is empty.
        TypeError
            If argument is not an integer or string.
        """
        if len(self) == 0:
            raise KeyError("Pipeline is empty.")
        if isinstance(key, int):
            key = self.node_key(key)
        elif isinstance(key, str):
            if key not in self.nodes:
                raise KeyError("Node not found in this pipeline.")
        else:
            raise TypeError("Argument must be a string or an integer.")
        return self.nodes[key]

    def build_struct(self, nodes=None):
        """Return networkx graph from pipeline computation structure.

        Return computation graph as a networkx DiGraph. If node list is not
        None, a graph including nodes in list is generated with all their
        immeadiate dependencies.

        Parameters
        ----------
        nodes: list
            A list of nodes to include in graph. If None, all nodes are
            included.

        Returns
        -------
        graph: networkx.DiGraph
            The resulting graph.

        Raises
        ------
        ValueError
            If argument 'nodes' is not None and is not a list or tuple.
        KeyError
            If node list is not a subset of pipeline node keys.
        """
        if nodes is not None:
            if not isinstance(nodes, (list, tuple)):
                message = "Argument 'nodes' must be a list."
                raise ValueError(message)

            if not set(nodes).issubset(set(self.keys())):
                message = "All strings in argument 'nodes' must be node keys."
                raise KeyError(message)
        else:
            nodes = self.keys()

        G = nx.DiGraph()

        for key in nodes:
            G.add_node(key, **self.nodes[key].meta)

        for key in nodes:
            for dkey in self.nodes_up[key]:
                if dkey is not None:
                    if dkey not in G:
                        G.add_node(dkey, **self.nodes[dkey].meta)
                    edge = (dkey, key)
                    G.add_edge(*edge)
            for dkey in self.nodes_down[key]:
                if dkey is not None:
                    if dkey not in G:
                        G.add_node(dkey, **self.nodes[dkey].meta)
                    edge = (key, dkey)
                    G.add_edge(*edge)
        return G

    def node_order(self, nodes=None):
        """Return node order within pipeline.

        The order of a node is the length of the longest path from any input.
        """
        if nodes is not None:
            if not isinstance(nodes, (list, tuple)):
                message = "Argument 'nodes' must be a list of node keys."
                raise ValueError(message)
            for key in nodes:
                if key not in self.nodes:
                    raise KeyError(f"Key {key} from arguments 'nodes' not " +
                                   "found within pipeline.")
        else:
            nodes = list(self.keys())

        G = self.struct
        operation_order = OrderedDict()
        all_inputs = self.inputs
        for key in nodes:
            max_path_length = 0
            if key not in all_inputs:
                for source in all_inputs:
                    path = self.shortest_path(source=source,
                                              target=key,
                                              nxgraph=G)
                    if path is not None:
                        path_length = len(path)
                        if path_length > max_path_length:
                            max_path_length = path_length
            operation_order[key] = max_path_length
        return operation_order

    def shortest_path(self, source, target, nxgraph=None):
        """Return shortest path from node 'source' to node 'target'."""
        if source not in self.nodes or target not in self.nodes:
            message = ("Arguments 'source' and 'target' must be valid node " +
                       "keys.")
            raise KeyError(message)

        if nxgraph is None:
            nxgraph = self.struct

        try:
            path = nx.shortest_path(nxgraph, source=source, target=target)
        except nx.NetworkXNoPath:
            path = None

        return path

    @property
    def struct(self):
        """Full networkx directed acyclic graph."""
        return self.build_struct()

    @property
    def outputs(self):
        """Pipeline outputs."""
        return [key for key in self.nodes if self.nodes[key].is_output]

    @property
    def inputs(self):
        inputs = [key for key in self.places
                  if len(self.nodes_up[key]) == 0]
        for node in inputs:
            yield node

    @property
    def operations(self):
        operations = [key for key in self.transitions]
        for node in operations:
            yield node

    @property
    def names(self):
        """Return iterator of names."""
        keys = list(self.keys())
        unique_names = list(set([self.nodes[key].name for key in keys]))
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
        if self.nodes[key].is_place:
            if self.nodes[key].parent is not None:
                parent_key = self.nodes[key].parent.key
                message = (f"This node is an output place for {parent_key}" +
                           f" transition. Delete node {parent_key} fisrt. " +
                           "If you want to set this place use " +
                           "pipeline['node_key'] = new_node")
                raise ValueError(message)
        self.nodes[key].clear()
        del self.nodes_up[key]
        del self.nodes_down[key]
        del self.nodes[key]

    def __setitem__(self, key, value):
        """Set node with key to value."""
        if not isinstance(value, Node):
            if key not in self.nodes:
                raise KeyError("Key not found. Setting node result value is" +
                               " only allowded for existing nodes.")
            self.nodes[key].set_value(value)
        else:
            if value in self:
                if value.key != key:
                    for node_key in self.nodes:
                        for ind, node in enumerate(self.nodes_up[node_key]):
                            if node == value.key:
                                self.nodes_up[node_key][ind] = key
                        for ind, node in enumerate(self.nodes_down[node_key]):
                            if node == value.key:
                                self.nodes_down[node_key][ind] = key
                    prev_key = value.key
                    self.nodes[key] = self.nodes.pop(prev_key)
                    self.nodes_up[key] = self.nodes_up.pop(prev_key)
                    self.nodes_down[key] = self.nodes_down.pop(prev_key)
                    if self.nodes[key].is_transition:
                        self.transitions[key] = self.transitions.pop(prev_key)
                    else:
                        self.places[key] = self.places.pop(prev_key)
                    self.nodes[key].refresh_key()
            else:
                value.set_pipeline(self)
                self.nodes[key] = value
                if self.nodes[key].is_transition:
                    self.transitions[key] = self.nodes[key]
                else:
                    self.places[key] = self.nodes[key]
                self.nodes[key].refresh_key()
                self.add_neighbours(key)

    def __iter__(self):
        """Return node iterator."""
        for key in self.nodes:
            yield key

    def __contains__(self, node):
        """Return true if item in pipeline."""
        if isinstance(node, Node):
            nodes = list(self.nodes.items())
            return node in [item[1] for item in nodes]
        return node in list(self.nodes.keys())

    def plot(self,
             ax=None,
             nodes=None,
             trans_color="white",
             trans_edge_color="black",
             trans_size=3000,
             trans_shape="s",
             place_size=3000,
             place_color="lightgrey",
             place_edge_color="black",
             place_shape="o",
             font_size=12,
             font_color="black",
             font_weight=1.0,
             font_family='sans-serif',
             font_alpha=1.0,
             min_target_margin=38,
             min_source_margin=15,
             head_length=0.6,
             head_width=0.8,
             tail_width=0.4,
             arrow_style='Fancy',
             arrow_width=3.0,
             graph_layout="dot",
             node_labels=None,
             **kwargs):
        """Plot pipeline's graph."""
        if graph_layout is None:
            graph_layout = "twopi"
        # use_graphviz = True
        # if hasattr(nx.drawing.layout, graph_layout):
        #     use_graphviz = False

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (20, 20)))

        G = self.build_struct(nodes)
        places = []
        transitions = []
        for key, meta in G.nodes(data=True):
            if meta["node_type"] == "transition":
                transitions.append(key)
            else:
                places.append(key)

        pos = graphviz_layout(G, prog=graph_layout, args='')
        # if use_graphviz:
        #     pos = graphviz_layout(G, prog=graph_layout, args='')
        # else:
        #     pos = getattr(nx.drawing.layout, graph_layout)

        nx.draw_networkx_nodes(G,
                               pos=pos,
                               ax=ax,
                               nodelist=places,
                               node_size=place_size,
                               node_color=place_color,
                               node_shape=place_shape,
                               edgecolors=place_edge_color)

        nx.draw_networkx_nodes(G,
                               pos=pos,
                               ax=ax,
                               nodelist=transitions,
                               node_size=trans_size,
                               node_color=trans_color,
                               node_shape=trans_shape,
                               edgecolors=trans_edge_color)

        astyle = mpl.patches.ArrowStyle(arrow_style,
                                        head_length=head_length,
                                        head_width=head_width,
                                        tail_width=tail_width)

        nx.draw_networkx_edges(G,
                               pos=pos,
                               ax=ax,
                               edge_color="black",
                               width=arrow_width,
                               arrowstyle=astyle,
                               min_target_margin=min_target_margin,
                               min_source_margin=min_source_margin)

        nx.draw_networkx_labels(G,
                                pos=pos,
                                ax=ax,
                                labels=node_labels,
                                font_size=font_size,
                                font_color=font_color,
                                font_weight=font_weight,
                                font_family=font_family,
                                alpha=font_alpha
                                )
        return ax

    @abstractmethod
    def get_node(self, key, feed=None, compute=False, force=False, **kwargs):
        """Get node from pipeline graph."""

    @abstractmethod
    def compute(self,
                nodes=None,
                feed=None,
                read=None,
                write=False,
                keep=None,
                force=False,
                **kwargs):
        """Compute pipeline."""
    #
    # @abstractmethod
    # def __copy__(self):
    #     """Return a shallow copy of self."""
