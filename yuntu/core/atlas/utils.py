"""Utilities for geometry manipulation."""
import math
import shapely.wkt
from shapely.geometry.polygon import Polygon
import matplotlib.pyplot as plt


def plot_geometry(geom, ax=None, outpath=None, **kwargs):
    """Plot geometry."""
    if isinstance(geom, Polygon):
        x, y = geom.exterior.xy
    else:
        x, y = geom.xy
    if ax is None:
        ax = plt.gca()
    ax.plot(x, y, **kwargs)
    if outpath is not None:
        plt.savefig(outpath)


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


def reference_system(time_win, time_hop,
                     freq_win, freq_hop,
                     bounds):
    """Produce abstract reference system."""
    ref_sys = {}
    tsteps = math.ceil((bounds[1] - bounds[0])/time_hop)
    fsteps = math.ceil((bounds[1] - bounds[0])/time_hop)
    for tstep in range(tsteps):
        start_time = bounds[0] + tstep * time_hop
        end_time = min(start_time + time_win, bounds[1])
        for fstep in range(fsteps):
            min_freq = bounds[2] + fstep * freq_hop
            max_freq = min(min_freq + freq_win, bounds[3])
            ref_sys[(tstep, fstep)] = (start_time, end_time,
                                       min_freq, max_freq)
    return ref_sys, (tsteps, fsteps)
