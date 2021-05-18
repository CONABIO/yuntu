from typing import Optional
from typing import Tuple
from functools import partial
from functools import wraps

import matplotlib.pyplot as plt
from matplotlib.axes import Axes


def get_ax(
    ax: Optional[Axes] = None,
    figsize: Optional[Tuple[float, float]] = (10, 5),
    **kwargs,
) -> Axes:
    if figsize is None:
        figsize = figsize

    if ax is not None:
        return ax

    return plt.subplots(figsize=figsize)[1]


class PlotterInterfase:
    def __init__(self, obj, plugin_mount):
        self.plugin_mount = plugin_mount
        self.obj = obj

    def list(self):
        return [
            plotter.__name__.lower() for plotter in self.plugin_mount.plugins
        ]

    def __repr__(self):
        return str(self.list())

    def __getattr__(self, name):
        try:
            return super().__getattr__(name)

        except AttributeError:
            pass

        for plugin in self.plugin_mount.plugins:
            plotter_name = plugin.__name__.lower()
            if plotter_name == name:
                break

        else:
            raise ValueError(
                f"No plotter with name {name} was found. "
                f"Available plotting functions: {self.list()}"
            )

        wrapper = wraps(plugin)(partial(plugin, self.obj))
        return wrapper


class plotter_descriptor:
    def __init__(self, plugin_mount):
        self.plugin_mount = plugin_mount

    def __get__(self, obj, objtype=None):
        return PlotterInterfase(obj, self.plugin_mount)
