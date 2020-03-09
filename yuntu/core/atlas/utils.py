"""Utilities for geometry manipulation."""
import shapely.wkt
from shapely.geometry.polygon import Polygon
import matplotlib.pyplot as plt


def plot_geometry(geom, ax=None, **kwargs):
    """Plot geometry."""
    if isinstance(geom, Polygon):
        x, y = geom.exterior.xy
    else:
        x, y = geom.xy
    if ax is None:
        ax = plt.gca()
    ax.plot(x, y, **kwargs)


def geom_from_wkt(wkt):
    """Parse and return geometry."""
    return shapely.wkt.loads(wkt)


def bbox_to_polygon(bbox):
    """Return polygon object from bounding box."""
    return Polygon([[bbox[0], bbox[2]],
                    [bbox[0], bbox[3]],
                    [bbox[1], bbox[3]],
                    [bbox[1], bbox[2]],
                    [bbox[0], bbox[2]]])
