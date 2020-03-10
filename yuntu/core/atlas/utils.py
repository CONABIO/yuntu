"""Utilities for geometry manipulation."""
import math
import numpy as np
import shapely.wkt
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry import LineString
from skimage.draw import polygon, line_aa
import matplotlib.pyplot as plt


def plot_geometry(geom, ax=None, outpath=None, **kwargs):
    """Plot geometry.

    Parameters
    ----------
    geom: shapely.geometry
        A geometry to plot.

    ax: matplotlib.pyplot.axis
        Plot axis.

    outpath: str
        Path to write plot.
    """
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
    """Parse and return geometry.

    Parameters
    ----------
    wkt: str
        String with geometry in wkt.

    Returns
    -------
    geometry: shapely.geometry
        Parsed geometry.
    """
    return shapely.wkt.loads(wkt)


def bbox_to_polygon(bbox):
    """Return polygon object from bounding box.

    Parameters
    ----------
    bbox: tuple(float, float, float, float)
        Bounding box as start_time, end_time, min_freq, max_freq.

    Returns
    -------
    polygon: shapely.geometry.polygon.Polygon
        Corresponding polygon.
    """
    return Polygon([[bbox[0], bbox[2]],
                    [bbox[0], bbox[3]],
                    [bbox[1], bbox[3]],
                    [bbox[1], bbox[2]],
                    [bbox[0], bbox[2]]])


def reference_system(time_win, time_hop,
                     freq_win, freq_hop,
                     bounds, center=None):
    """Produce abstract reference system.

    Using specified time and frequency windows and hops, build a regular grid
    between boundaries.

    Parameters
    ----------
    time_win: float
        Size of time window.

    time_hop: float
        Size of time hop.

    freq_win: float
        Size of frequency window.

    freq_hop: float
        Size of frequency hop.

    bounds: tuple(float, float, float, float)
        The extent of the reference system as start_time, end_time,
        min_freq, max_freq.

    Returns
    -------
    ref_sys: dict
        A dictionary with tuples of integer coordinates as keys and charts
        as attributes.

    shape: tuple(int, int)
        The shape of the corresponding dictionary.

    xrange: tuple(int, int)
        Limit values for coordinate system at axis x.

    yrange: tuple(int, int)
        Limit values for coordinate system at axis y.
    """
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


def build_multigeometry(geom_arr, geom_type="Polygon"):
    """Build multigeometry from geometry array.

    Parameters
    ----------
    geom_arr: list
        List of geometries to aggregate.

    geom_type: str
        Input geometry type.

    Returns
    -------
    geometry: shapely.geometry
        Multigeometry of aggregated input geometries.

    Raises
    ------
    NotImplementedError
        If geometry type is not supported.
    """
    if geom_type not in ["Polygon", "LineString"]:
        raise NotImplementedError("Geometry type not implemented.")
    if geom_type == "Polygon":
        return MultiPolygon(geom_arr)
    return MultiLineString(geom_arr)


def freq_to_bins(freq,
                 fbins,
                 frange,
                 fmin):
    fbin0 = min(fbins - 1,
                int(max(round(((freq - fmin) / frange) * fbins), 0)))
    return fbin0


def time_to_bins(time,
                 tbins,
                 trange):
    tbin0 = min(tbins - 1,
                int(max(round((time / trange) * tbins), 0)))
    return tbin0


def linestring_to_mask(geom,
                       shape,
                       trange,
                       frange,
                       fmin):
    """Rasterize linestring to binary mask of shape 'shape'."""
    tbins, fbins = shape
    times, freqs = geom.xy
    times_ = [time_to_bins(t, tbins, trange) for t in times]
    freqs_ = [freq_to_bins(f, fbins, frange, fmin) for f in freqs]
    mask = np.zeros(shape)
    for i in range(0, len(times_)-1):
        rr, cc, val = line_aa(times_[i], freqs_[i], times_[i+1], freqs_[i+1])
        mask[rr, cc] = 1
    return mask


def polygon_to_mask(geom,
                    shape,
                    trange,
                    frange,
                    fmin):
    """Rasterize polygon to binary mask of shape 'shape'."""
    tbins, fbins = shape
    times, freqs = geom.exterior.xy
    times_ = [time_to_bins(t, tbins, trange) for t in times]
    freqs_ = [freq_to_bins(f, fbins, frange, fmin) for f in freqs]
    rr, cc = polygon(times_, freqs_)
    mask = np.zeros(shape)
    mask[rr, cc] = 1
    return mask


def geometry_to_mask(geom,
                     shape,
                     trange,
                     frange,
                     fmin):
    """Rasterize geometry."""
    if isinstance(geom, Polygon):
        return polygon_to_mask(geom,
                               shape,
                               trange,
                               frange,
                               fmin)
    if isinstance(geom, LineString):
        return linestring_to_mask(geom,
                                  shape,
                                  trange,
                                  frange,
                                  fmin)
    raise NotImplementedError("Method not implemented for this kind of" +
                              "geometry")
