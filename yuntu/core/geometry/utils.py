"""Utilities for geometry manipulation."""
import math
from enum import Enum

import numpy as np
from scipy.signal import convolve2d
import matplotlib.pyplot as plt
import shapely.wkt
from shapely.geometry import box
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.linestring import LineString
import shapely.affinity as shapely_affinity
import shapely.ops as shapely_ops
from skimage.draw import polygon, line, circle


def point_geometry(x, y):
    """Return point geometry.

    Parameters
    ----------
    x: number
    y: number

    Returns
    -------
    geometry: shapely.geometry.Point
        Parsed geometry.
    """
    return Point(x, y)


def linestring_geometry(vertices):
    """Return line geometry.

    Parameters
    ----------
    vertices: numpy.array | list
        Array of vertices.

    Returns
    -------
    geometry: shapely.geometry.LineString
        Parsed geometry.
    """
    return LineString(vertices)


def linestring_set_zcoords(geom, zcoords):
    """Set third coordinate of linestring.

    Parameters
    ----------
    geom: shapely.geometry.linestring.Linestring
        The target geometry.
    zcoords: np.array | list
        Values for third coordinate.

    Returns
    -------
    geometry: shapely.geometry.linestring.LineString
        Geometry with new values on third dimension.

    Raises
    ------
    ValueError
        If input geometry is not of type LineString or input values for third
        dimension have wrong length.
    """
    if not isinstance(geom, LineString):
        message = 'Input geometry must be of type LineString'
        raise ValueError(message)
    x, y = geom.xy
    if len(x) != len(zcoords):
        message = 'Argument "zcoords" should correspond to LineString length'
        raise ValueError(message)
    vertices = [(x[i], y[i], zcoords[i]) for i in range(len(x))]
    return linestring_geometry(vertices)


def polygon_geometry(shell, holes=None):
    """Return polygon geometry.

    Parameters
    ----------
    shell: list
        Ordered sequence of points.
    holes:
        Sequence of rings.

    Returns
    -------
    geometry: shapely.geometry.polygon.Polygon
        New polygon geometry.
    """
    return Polygon(shell, holes)


def buffer_geometry(geom, buffer):
    """Apply buffer and return resulting geometry.

    Parameters
    ----------
    geom: shapely.geometry
        Target geometry.
    buffer: tuple(float, float)
        Buffer to apply at each dimension as a tuple.

    Returns
    -------
    geometry: shapely.geometry
        New geometry with buffer applied on each dimension.
    """
    ratio = buffer[1] / buffer[0]
    geometry = shapely_affinity.scale(geom, xfact=ratio, origin=(0, 0))
    geometry = geometry.buffer(buffer[1],
                               cap_style=1,
                               join_style=1)
    return shapely_affinity.scale(geometry, xfact=1/ratio, origin=(0, 0))


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


def bbox_geometry(minx, miny, maxx, maxy, ccw=True):
    """Return polygon object from box.

    Makes a rectangular polygon from the provided
    bounding box values, with counter-clockwise order by default.

    Parameters
    ----------
    minx: float
    miny: float
    maxx: float
    maxy: float

    Returns
    shapely.geometry.polygon.Polygon
    """
    return box(minx, miny, maxx, maxy, ccw=True)


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
        raise NotImplementedError("Geometry type not supported.")
    if geom_type == "Polygon":
        return MultiPolygon(geom_arr)
    return MultiLineString(geom_arr)


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


def translate_geometry(geom, xoff=0.0, yoff=0.0, zoff=0.0):
    """Shift geometry by specified offset on each dimension.

    Parameters
    ----------
    geom: shapely.geometry
        A geometry.

    xoff: float
        Offset for first dimension.
    yoff: float
        Offset for second dimension.
    zoff: float
        Offset for third dimension.

    Returns
    -------
    geometry: shapely.geometry
        The transformed geometry.
    """
    return shapely_affinity.translate(geom, xoff, yoff, zoff)


def scale_geometry(geom, xfact=1.0, yfact=1.0, origin='center'):
    """Scale geometry by the specified factor on each dimension.

    Parameters
    ----------
    geom: shapely.geometry
        A geometry.

    xfact: float
        Transformation factor for first dimension.
    yfact: float
        Transformation factor for second dimension.
    zfact: float
        Transformation factor for third dimension.
    origin: str
        Reference point.

    Returns
    -------
    geometry: shapely.geometry
        The transformed geometry.
    """
    return shapely_affinity.scale(geom, xfact, yfact, origin=origin)


def transform_geometry(geom, func):
    """Transform geometry by given function.

    Parameters
    ----------
    geom: shapely.geometry
        A geometry.

    func: function
        A transformation to apply by coordinate. It should return a tuple of
        new values for each dimension.

    outpath: str
        Path to write plot.

    Returns
    -------
    geometry: shapely.geometry
        The transformed geometry.
    """
    return shapely_ops.transform(func, geom)


def linestring_to_mask(geom,
                       shape,
                       transformX=None,
                       transformY=None):
    """Rasterize linestring to binary mask of shape 'shape'.

    Parameters
    ----------
    geom: shapely.geometry.LineString
        LineString to rasterize.
    shape: tuple(int, int)
        Shape of output mask.
    transformX: function
        Transformation to apply on 'x' coordinates.
    transformY: function
        Transformation to apply on 'y' coordinates.

    Returns
    -------
    mask: np.array
        Resulting mask.
    """
    x, y = geom.xy

    if transformX is not None:
        x = np.array([transformX(z) for z in x])

    if transformY is not None:
        y = np.array([transformY(z) for z in y])

    mask = np.zeros(shape, dtype=bool)
    for i in range(0, len(x)-1):
        rr, cc = line(y[i], x[i], y[i+1], x[i+1])
        mask[rr, cc] = True

    return mask


def polygon_to_mask(geom,
                    shape,
                    transformX=None,
                    transformY=None):
    """Rasterize polygon to binary mask of shape 'shape'.

    Parameters
    ----------
    geom: shapely.geometry.polygon.Polygon
        Polygon to rasterize.
    shape: tuple(int, int)
        Shape of output mask.
    transformX: function
        Transformation to apply on 'x' coordinates.
    transformY: function
        Transformation to apply on 'y' coordinates.

    Returns
    -------
    mask: np.array
        Resulting mask.
    """
    x, y = geom.exterior.xy
    if transformX is not None:
        x = np.array([transformX(z) for z in x])
    if transformY is not None:
        y = np.array([transformY(z) for z in y])

    rr, cc = polygon(y, x, shape=shape)
    mask = np.zeros(shape, dtype=bool)
    mask[rr, cc] = True

    for interior in geom.interiors:
        x, y = interior.xy
        if transformX is not None:
            x = np.array([transformX(z) for z in x])
        if transformY is not None:
            y = np.array([transformY(z) for z in y])
        rr, cc = polygon(y, x, shape=shape)
        mask[rr, cc] = False

    return mask


def geometry_to_mask(geom,
                     shape,
                     transformX=None,
                     transformY=None):
    """Rasterize geometry.

    Parameters
    ----------
    geom: shapely.geometry
        Geometry to rasterize.
    shape: tuple(int, int)
        Shape of output mask.
    transformX: function
        Transformation to apply on 'x' coordinates.
    transformY: function
        Transformation to apply on 'y' coordinates.

    Returns
    -------
    mask: np.array
        Resulting mask.
    """
    if isinstance(geom, Polygon):
        return polygon_to_mask(geom=geom,
                               shape=shape,
                               transformX=transformX,
                               transformY=transformY)
    if isinstance(geom, LineString):
        return linestring_to_mask(geom=geom,
                                  shape=shape,
                                  transformX=transformX,
                                  transformY=transformY)
    raise NotImplementedError("Method not implemented for this kind of" +
                              "geometry")


def point_neighbourhood(array,
                        point,
                        bins=1,
                        transformX=None,
                        transformY=None):
    """Get neighbourhood values at point with bin buffer.

    Parameters
    ----------
    array: np.array
        Array of values to query.
    point: tuple
        Point coordinates.
    bins: int
        Discrete bin buffer to apply.
    transformX: function
        Transformation to apply on 'x' coordinates.
    transformY: function
        Transformation to apply on 'y' coordinates.

    Returns
    -------
    values: np.array
        Values of all entries within the neighbourhood.
    """
    if not isinstance(point, (tuple, list)):
        message = 'Point argument must be a tuple or a list'
        raise ValueError(message)
    if not len(point) == 2:
        message = 'Point argument should be two dimensional'
        raise ValueError(message)
    if not isinstance(bins, int):
        message = 'Bins argument should be of type integer'
        raise ValueError(message)
    if bins <= 0:
        message = 'Bins argument must be greater than 0'
        raise ValueError(message)

    if transformX is not None and transformY is not None:
        point = (transformX(point[0]), transformY(point[1]))
    elif transformX is not None:
        point = (transformX(point[0]), point[1])
    elif transformY is not None:
        point = (point[0], transformY(point[1]))

    X, Y = point
    rr, cc = circle(Y, X, bins, array.shape)
    return array[rr, cc]


def geometry_neighbourhood(array,
                           geom,
                           bins=0,
                           transformX=None,
                           transformY=None):
    """Get neighbourhood values from geometry.

    Parameters
    ----------
    array: np.array
        Array of values to query.
    geom: shapely.geometry
        Geometry to use as base neighbourhood.
    bins: int
        Discrete bin buffer to apply.
    transformX: function
        Transformation to apply on 'x' coordinates.
    transformY: function
        Transformation to apply on 'y' coordinates.

    Returns
    -------
    values: np.array
        Values of all entries within the neighbourhood.
    """
    if bins is None:
        bins = 0

    mask = geometry_to_mask(geom,
                            array.shape,
                            transformX,
                            transformY)

    if bins != 0:
        if not isinstance(bins, int):
            message = 'Bins argument should be of type integer'
            raise ValueError(message)
        if bins < 0:
            message = 'Bins argument must be non negative'
            raise ValueError(message)
        kernel = np.ones([bins, bins])
        mask = convolve2d(mask, kernel, mode='same') > 0
    return array[mask]


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


def point_buffer(time, freq, buffer):
    """Get a buffer around a time-frequency point."""
    if isinstance(buffer, (int, float)):
        buffer = [buffer, buffer]

    if not isinstance(buffer, (tuple, list)):
        message = 'The buffer argument must be a number, a tuple or a list'
        raise ValueError(message)

    point = Point(time, freq)
    return buffer_geometry(point, buffer)
