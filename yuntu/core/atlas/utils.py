"""Utilities for geometry manipulation."""
import math
import numpy as np
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
                     bounds, center=None):
    """Produce abstract reference system."""
    ref_sys = {}
    boxes = []
    if center is None:
        tsteps = math.ceil((bounds[1] - bounds[0])/time_hop)
        fsteps = math.ceil((bounds[1] - bounds[0])/freq_hop)
        for tstep in range(tsteps):
            start_time = bounds[0] + tstep * time_hop
            end_time = min(start_time + time_win, bounds[1])
            for fstep in range(fsteps):
                min_freq = bounds[2] + fstep * freq_hop
                max_freq = min(min_freq + freq_win, bounds[3])
                limits = (start_time, end_time, min_freq, max_freq)
                if limits not in boxes:
                    boxes.append(limits)
                    ref_sys[(tstep, fstep)] = limits
    else:
        tsteps_l = math.ceil((center[0] - bounds[0])/time_hop)
        tsteps_r = math.ceil((bounds[1] - center[0])/time_hop)
        fsteps_b = math.ceil((center[1] - bounds[2])/freq_hop)
        fsteps_t = math.ceil((bounds[3] - center[1])/freq_hop)
        for tstep in range(-tsteps_l, tsteps_r + 1):
            start_time = max(bounds[0], center[0] + tstep * time_hop)
            end_time = min(start_time + time_win, bounds[1])
            if end_time - start_time > 0:
                for fstep in range(-fsteps_b, fsteps_t + 1):
                    min_freq = max(bounds[2], center[1] + fstep * freq_hop)
                    max_freq = min(min_freq + freq_win, bounds[3])
                    if max_freq - min_freq > 0:
                        limits = (start_time, end_time, min_freq, max_freq)
                        if limits not in boxes:
                            boxes.append(limits)
                            ref_sys[(tstep, fstep)] = limits
    cells = np.array(list(ref_sys.keys()))
    max_cells = np.amax(cells, axis=0)
    min_cells = np.amin(cells, axis=0)
    return ref_sys, cells.shape, (min_cells[0], max_cells[0]), \
                                 (min_cells[1], max_cells[1])
