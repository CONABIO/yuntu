"""Yuntu Geometries Module.

This module defines the base Geometries to be used for all
Yuntu objects. A geometry is a region of coordinate space with
time and/or frequency as axis.
"""
from abc import ABC
from abc import abstractmethod
from enum import Enum

import numpy as np

import yuntu.core.windows as windows
import yuntu.core.geometry.utils as geom_utils

INFINITY = 10e+15


def _parse_args(arg, method, argname, index=0, **kwargs):
    if arg is None:
        if argname not in kwargs:
            message = (
                f'Either {method} or freq arguments must '
                f'be supplied to the {method} method.')
            raise ValueError(message)
        freq = kwargs[argname]

    elif isinstance(arg, (float, int)):
        freq = arg

    elif isinstance(arg, (list, float)):
        freq = arg[index]

    else:
        message = (
            f'The {method} argument should be a number of a tuple/list')
        raise ValueError(message)

    return freq


def _parse_tf(arg, method, default=0, **kwargs):
    if isinstance(arg, (int, float)):
        arg = [arg, arg]

    try:
        time = _parse_args(arg, method, 'time', **kwargs)
    except ValueError:
        time = default

    try:
        freq = _parse_args(arg, method, 'freq', index=1, **kwargs)
    except ValueError:
        freq = default

    return time, freq


class Geometry(ABC):
    name = None

    class Types(Enum):
        Weak = 'Weak'
        TimeLine = 'TimeLine'
        TimeInterval = 'TimeInterval'
        FrequencyLine = 'FrequencyLine'
        FrequencyInterval = 'FrequencyInterval'
        BBox = 'BBox'
        Point = 'Point'
        LineString = 'LineString'
        Polygon = 'Polygon'
        MultiPoint = 'MultiPoint'
        MultiLineString = 'MultiLineString'
        MultiPolygon = 'MultiPolygon'

    def __init__(self, geometry=None):
        self.geometry = geometry

    def __repr__(self):
        args = ', '.join([
            '{}={}'.format(key, value)
            for key, value in self.to_dict().items()
            if key != 'type'
        ])
        name = type(self).__name__
        return f'{name}({args})'

    @property
    def window(self):
        left, bottom, right, top = self.bounds

        if left is None or right is None:
            return windows.FrequencyWindow(min=bottom, max=top)

        if bottom is None or top is None:
            return windows.TimeWindow(start=left, end=right)

        return windows.TimeFrequencyWindow(
            min=bottom, max=top, start=left, end=right)

    def buffer(self, buffer=None, **kwargs):
        time, freq = _parse_tf(buffer, 'buffer', **kwargs)
        line_buffer = geom_utils.buffer_geometry(
            self.geometry,
            buffer=[time, freq])
        return Polygon(geometry=line_buffer)

    def shift(self, shift=None, **kwargs):
        time, freq = _parse_tf(shift, 'shift', **kwargs)
        translated = geom_utils.translate_geometry(
            self.geometry,
            xoff=time,
            yoff=freq)
        return type(self)(geometry=translated)

    def scale(self, scale=None, center=None, **kwargs):
        time, freq = _parse_tf(scale, 'scale', default=1, **kwargs)

        if center is None:
            center = 'center'

        scaled = geom_utils.scale_geometry(
            self.geometry,
            xfact=time,
            yfact=freq,
            origin=center)
        return type(self)(geometry=scaled)

    def transform(self, transform):
        transformed = geom_utils.transform_geometry(
            self.geometry,
            transform)
        return type(self)(geometry=transformed)

    def intersects(self, other):
        if isinstance(other, (windows.Window, Geometry)):
            return self.geometry.intersects(other.geometry)

        return self.geometry.intersects(other)

    @abstractmethod
    def plot(self, ax=None, **kwargs):
        pass

    @property
    def bounds(self):
        return self.geometry.bounds

    def to_dict(self):
        return {'type': self.name.value}

    @staticmethod
    def from_dict(data):
        data = data.copy()
        geom_type = Geometry.Types[data.pop('type')]

        if geom_type == Geometry.Types.Weak:
            return Weak(**data)

        if geom_type == Geometry.Types.TimeLine:
            return TimeLine(**data)

        if geom_type == Geometry.Types.TimeInterval:
            return TimeInterval(**data)

        if geom_type == Geometry.Types.FrequencyLine:
            return FrequencyLine(**data)

        if geom_type == Geometry.Types.FrequencyInterval:
            return FrequencyInterval(**data)

        if geom_type == Geometry.Types.Point:
            return Point(**data)

        if geom_type == Geometry.Types.BBox:
            return BBox(**data)

        if geom_type == Geometry.Types.LineString:
            return LineString(**data)

        if geom_type == Geometry.Types.Polygon:
            return Polygon(**data)

        raise NotImplementedError


class Weak(Geometry):
    name = Geometry.Types.Weak

    def __init__(self, geometry=None):
        geometry = geom_utils.bbox_to_polygon([
            0, INFINITY,
            0, INFINITY
        ])
        super().__init__(geometry=geometry)

    def buffer(self, buffer=None, **kwargs):
        return self

    def shift(self, shift=None, **kwargs):
        return self

    def scale(self, scale=None, center=None, **kwargs):
        return self

    def transform(self, transform):
        return self

    def plot(self, ax=None, **kwargs):
        return ax

    @property
    def bounds(self):
        return None, None, None, None


class TimeLine(Geometry):
    name = Geometry.Types.TimeLine

    def __init__(self, time=None, geometry=None):
        if geometry is None:
            geometry = geom_utils.linestring_geometry([
                (time, 0),
                (time, INFINITY)])

        super().__init__(geometry=geometry)

        time, _, _, _ = self.geometry.bounds
        self.time = time

    def to_dict(self):
        data = super().to_dict()
        data['time'] = self.time
        return data

    @property
    def bounds(self):
        return self.time, None, self.time, None

    def buffer(self, buffer=None, **kwargs):
        time = _parse_args(buffer, 'buffer', 'time', **kwargs)
        start_time = self.time - time
        end_time = self.time + time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def shift(self, shift=None, **kwargs):
        time = _parse_args(shift, 'shift', 'time', **kwargs)
        time = self.time + time
        return TimeLine(time=time)

    def scale(self, scale=None, center=None, **kwargs):
        return self

    def transform(self, transform):
        time = transform(self.time)
        return TimeLine(time=time)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax.axvline(
            self.time,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', None),
            label=kwargs.get('label', None))

        return ax


class TimeInterval(Geometry):
    name = Geometry.Types.TimeInterval

    def __init__(self, start_time=None, end_time=None, geometry=None):
        if geometry is None:
            geometry = geom_utils.bbox_to_polygon([
                start_time, end_time,
                0, INFINITY])

        super().__init__(geometry=geometry)

        start_time, _, end_time, _ = self.geometry.bounds
        self.end_time = end_time
        self.start_time = start_time

    def to_dict(self):
        data = super().to_dict()
        data['start_time'] = self.start_time
        data['end_time'] = self.end_time
        return data

    @property
    def bounds(self):
        return self.start_time, None, self.end_time, None

    @property
    def start_line(self):
        return TimeLine(time=self.start_time)

    @property
    def end_line(self):
        return TimeLine(time=self.end_time)

    @property
    def center_line(self):
        center = (self.start_time + self.end_time) / 2
        return TimeLine(time=center)

    def buffer(self, buffer=None, **kwargs):
        time = _parse_args(buffer, 'buffer', 'time', **kwargs)
        start_time = self.start_time - time
        end_time = self.end_time + time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def shift(self, shift=None, **kwargs):
        time = _parse_args(shift, 'shift', 'time', **kwargs)
        start_time = self.start_time + time
        end_time = self.end_time + time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def scale(self, scale=None, center=None, **kwargs):
        time = _parse_args(scale, 'scale', 'time', **kwargs)

        if center is None:
            center = (self.start_time + self.end_time) / 2
        elif isinstance(center, (list, tuple)):
            center = center[0]

        start_time = center + (self.start_time - center) * time
        end_time = center + (self.end_time - center) * time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def transform(self, transform):
        start_time = transform(self.start_time)
        end_time = transform(self.end_time)
        return TimeInterval(start_time=start_time, end_time=end_time)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        color = kwargs.get('color', None)
        if color is None:
            color = next(ax._get_lines.prop_cycler)['color']

        ax.axvline(
            self.start_time,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        ax.axvline(
            self.end_time,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        if kwargs.get('fill', True):
            ax.axvspan(
                self.start_time,
                self.end_time,
                alpha=kwargs.get('alpha', 0.5),
                color=color,
                label=kwargs.get('label', None))

        return ax


class FrequencyLine(Geometry):
    name = Geometry.Types.FrequencyLine

    def __init__(self, freq=None, geometry=None):
        if geometry is None:
            geometry = geom_utils.linestring_geometry([
                (0, freq),
                (INFINITY, freq)])

        super().__init__(geometry=geometry)

        _, freq, _, _ = self.geometry.bounds
        self.freq = freq

    def to_dict(self):
        data = super().to_dict()
        data['freq'] = self.freq
        return data

    @property
    def bounds(self):
        return None, self.freq, None, self.freq

    def buffer(self, buffer=None, **kwargs):
        freq = _parse_args(buffer, 'buffer', 'freq', index=1, **kwargs)
        min_freq = self.freq - freq
        max_freq = self.freq + freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def shift(self, shift=None, **kwargs):
        freq = _parse_args(shift, 'shift', 'freq', index=1, **kwargs)
        freq = self.freq + freq
        return FrequencyLine(freq=freq)

    def scale(self, scale=None, center=None, **kwargs):
        return self

    def transform(self, transform):
        freq = transform(self.freq)
        return FrequencyLine(freq=freq)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax.axhline(
            self.freq,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', None),
            label=kwargs.get('label', None))

        return ax


class FrequencyInterval(Geometry):
    name = Geometry.Types.FrequencyInterval

    def __init__(self, min_freq=None, max_freq=None, geometry=None):
        if geometry is None:
            geometry = geom_utils.bbox_to_polygon([
                0, INFINITY,
                min_freq, max_freq
            ])

        super().__init__(geometry=geometry)

        _, min_freq, _, max_freq = self.geometry.bounds
        self.min_freq = min_freq
        self.max_freq = max_freq

    def to_dict(self):
        data = super().to_dict()
        data['min_freq'] = self.min_freq
        data['max_freq'] = self.max_freq
        return data

    @property
    def bounds(self):
        return None, self.min_freq, None, self.max_freq

    @property
    def min_line(self):
        return FrequencyLine(freq=self.min_freq)

    @property
    def max_line(self):
        return FrequencyLine(freq=self.max_freq)

    @property
    def center_line(self):
        center = (self.min_freq + self.max_freq) / 2
        return FrequencyLine(freq=center)

    def buffer(self, buffer=None, **kwargs):
        freq = _parse_args(buffer, 'buffer', 'freq', index=1, **kwargs)
        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def shift(self, shift=None, **kwargs):
        freq = _parse_args(shift, 'shift', 'freq', index=1, **kwargs)
        min_freq = self.min_freq + freq
        max_freq = self.max_freq + freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def scale(self, scale=None, center=None, **kwargs):
        freq = _parse_args(scale, 'scale', 'freq', index=1, **kwargs)

        if center is None:
            center = (self.min_freq + self.max_freq) / 2
        elif isinstance(center, (list, tuple)):
            center = center[0]

        min_freq = center + (self.min_freq - center) * freq
        max_freq = center + (self.max_freq - center) * freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def transform(self, transform):
        min_freq = transform(self.min_freq)
        max_freq = transform(self.max_freq)
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        color = kwargs.get('color', None)
        if color is None:
            color = next(ax._get_lines.prop_cycler)['color']

        ax.axhline(
            self.min_freq,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        ax.axhline(
            self.max_freq,
            linewidth=kwargs.get('linewidth', None),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        if kwargs.get('fill', True):
            ax.axhspan(
                self.min_freq,
                self.max_freq,
                alpha=kwargs.get('alpha', 0.5),
                color=color,
                label=kwargs.get('label', None))

        return ax


class Point(Geometry):
    name = Geometry.Types.Point

    def __init__(self, time=None, freq=None, geometry=None):
        if geometry is None:
            geometry = geom_utils.point_geometry(time, freq)

        super().__init__(geometry=geometry)

        self.time = self.geometry.x
        self.freq = self.geometry.y

    def __getitem__(self, key):
        if not isinstance(key, int):
            message = f'Index must be integer not {type(key)}'
            raise ValueError(message)

        if key < 0 or key > 1:
            raise IndexError

        if key == 0:
            return self.time

        return self.freq

    def to_dict(self):
        data = super().to_dict()
        data['time'] = self.time
        data['freq'] = self.freq
        return data

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax.scatter(
            [self.time],
            [self.freq],
            s=kwargs.get('size', None),
            color=kwargs.get('color', None),
            marker=kwargs.get('marker', None))

        return ax


class BBox(Geometry):
    name = Geometry.Types.BBox

    def __init__(
            self,
            start_time=None,
            end_time=None,
            min_freq=None,
            max_freq=None,
            geometry=None):
        if geometry is None:
            if start_time is None:
                message = 'Bounding box start time must be set.'
                raise ValueError(message)

            if end_time is None:
                message = 'Bounding box end time must be set.'
                raise ValueError(message)

            if min_freq is None:
                message = 'Bounding box max freq must be set.'
                raise ValueError(message)

            if max_freq is None:
                message = 'Bounding box min freq must be set.'
                raise ValueError(message)

            geometry = geom_utils.bbox_to_polygon([
                start_time, end_time,
                min_freq, max_freq])

        super().__init__(geometry=geometry)

        start_time, min_freq, end_time, max_freq = self.geometry.bounds
        self.start_time = start_time
        self.end_time = end_time
        self.min_freq = min_freq
        self.max_freq = max_freq

    def to_dict(self):
        data = super().to_dict()
        data['start_time'] = self.start_time
        data['end_time'] = self.end_time
        data['min_freq'] = self.min_freq
        data['max_freq'] = self.max_freq
        return data

    @property
    def center(self):
        time = (self.start_time + self.end_time) / 2
        freq = (self.min_freq + self.max_freq) / 2
        return Point(time=time, freq=freq)

    def buffer(self, buffer=None, **kwargs):
        time, freq = _parse_tf(buffer, 'buffer', default=0, **kwargs)
        start_time = self.start_time - time
        end_time = self.end_time + time
        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq
        return BBox(
            start_time=start_time,
            end_time=end_time,
            min_freq=min_freq,
            max_freq=max_freq)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        xcoords, ycoords = self.geometry.exterior.xy
        lineplot, = ax.plot(
            xcoords,
            ycoords,
            linewidth=kwargs.get('linewidth', None),
            color=kwargs.get('color', None),
            linestyle=kwargs.get('linestyle', '--'),
        )

        color = lineplot.get_color()

        if kwargs.get('fill', True):
            ax.fill(
                xcoords,
                ycoords,
                color=color,
                alpha=kwargs.get('alpha', 0.5),
                linewidth=0,
                label=kwargs.get('label', None))

        return ax


class LineString(Geometry):
    name = Geometry.Types.LineString

    def __init__(self, wkt=None, vertices=None, geometry=None):
        if geometry is None:
            if wkt is not None:
                geometry = geom_utils.geom_from_wkt(wkt)

            elif vertices is not None:
                geometry = geom_utils.linestring_geometry(vertices)

            else:
                message = (
                    'Either wkt or vertices must be supplied '
                    'to create a LineString geometry.')
                raise ValueError(message)

        super().__init__(geometry=geometry)

        self.wkt = self.geometry.wkt

    def __iter__(self):
        for (x, y) in self.geometry.coords:
            yield Point(x, y)

    def __getitem__(self, key):
        x, y = self.geometry.coords[key]
        return Point(x, y)

    @property
    def start(self):
        return self[0]

    @property
    def end(self):
        return self[-1]

    def to_dict(self):
        data = super().to_dict()
        data['wkt'] = self.wkt
        return data

    def interpolate(self, s, normalized=True):
        point = self.geometry.interpolate(s, normalized=normalized)
        return Point(point.x, point.y)

    def resample(self, num_samples):
        vertices = [
            self.interpolate(param)
            for param in np.linspace(0, 1, num_samples, endpoint=True)
        ]
        return LineString(vertices=vertices)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        xcoords, ycoords = self.geometry.xy
        lineplot, = ax.plot(
            xcoords,
            ycoords,
            linewidth=kwargs.get('linewidth', None),
            color=kwargs.get('color', None),
            linestyle=kwargs.get('linestyle', '--'),
        )

        color = lineplot.get_color()

        if kwargs.get('scatter', False):
            ax.scatter(
                xcoords,
                ycoords,
                color=color,
                s=kwargs.get('size', None))

        return ax


class Polygon(Geometry):
    name = Geometry.Types.Polygon

    def __init__(self, wkt=None, shell=None, holes=None, geometry=None):
        if geometry is None:
            if wkt is not None:
                geometry = geom_utils.geom_from_wkt(wkt)

            elif shell is not None:
                if holes is None:
                    holes = []

                geometry = geom_utils.polygon_geometry(shell, holes)

        super().__init__(geometry=geometry)

        self.wkt = self.geometry.wkt

    def to_dict(self):
        data = super().to_dict()
        data['wkt'] = self.wkt
        return data

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        lineplot, = ax.plot(
            *self.geometry.exterior.xy,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', None),
            linestyle=kwargs.get('linestyle', '--'),
        )

        color = lineplot.get_color()

        for interior in self.geometry.interiors:
            ax.plot(
                *interior.xy,
                linewidth=kwargs.get('linewidth', None),
                color=color,
                linestyle=kwargs.get('linestyle', '--'),
            )

        if kwargs.get('fill', True):
            if self.geometry.exterior.is_ccw:
                coords = self.geometry.exterior.coords
            else:
                coords = self.geometry.exterior.coords[::-1]

            xcoords, ycoords = map(list, zip(*coords))
            xstart = xcoords[0]
            ystart = ycoords[0]

            for interior in self.geometry.interiors:
                if interior.is_ccw:
                    coords = interior.coords[::-1]
                else:
                    coords = interior.coords

                intxcoords, intycoords = map(list, zip(*coords))
                xcoords += intxcoords
                ycoords += intycoords

                xcoords.append(xstart)
                ycoords.append(ystart)

            ax.fill(
                xcoords,
                ycoords,
                color=color,
                alpha=kwargs.get('alpha', 0.5),
                linewidth=0)

        return ax
