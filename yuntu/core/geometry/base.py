"""Yuntu Geometries Module.

This module defines the base Geometries to be used for all
Yuntu objects. A geometry is a region of coordinate space with
time and/or frequency as axis.
"""
from abc import ABC
from abc import abstractmethod
from enum import Enum

import yuntu.core.geometry.utils as geom_utils

INFINITY = 10e+20


class Geometry(ABC):
    name = None

    class Types(Enum):
        WEAK = 'Weak'
        TIMEINTERVAL = 'TimeInterval'
        FREQUENCYINTERVAL = 'FrequencyInterval'
        LINESTRING = 'LineString'
        POINT = 'Point'
        POLYGON = 'Polygon'
        MULTIPOINT = 'MultiPoint'
        MULTIPOLYGON = 'MultiPolygon'
        MULTILINESTRING = 'MultiLineString'

    def __init__(self, geometry=None):
        self.geometry = geometry

    @abstractmethod
    def buffer(self, buffer=None, **kwargs):
        pass

    @abstractmethod
    def shift(self, shift=None, **kwargs):
        pass

    @abstractmethod
    def dilate(self, dilate=None, center=None, **kwargs):
        pass

    @abstractmethod
    def transform(self, transform):
        pass

    @abstractmethod
    def plot(self, ax=None, **kwargs):
        pass

    @property
    def bounds(self):
        return self.geometry.bounds

    @property
    def top(self):
        return self.bounds[3]

    @property
    def bottom(self):
        return self.bounds[1]

    @property
    def left(self):
        return self.bounds[0]

    @property
    def right(self):
        return self.bounds[2]

    def to_dict(self):
        return {
            'type': self.name.value
        }

    @staticmethod
    def from_dict(data):
        data = data.copy()
        geom_type = Geometry.Types[data.pop('type')]

        if geom_type == Geometry.Types.WEAK:
            return Weak(**data)

        if geom_type == Geometry.Types.TIMEINTERVAL:
            return TimeInterval(**data)

        raise NotImplementedError


class Weak(Geometry):
    name = Geometry.Types.WEAK

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

    def dilate(self, dilate=None, center=None, **kwargs):
        return self

    def transform(self, transform):
        return self

    def plot(self, ax=None, **kwargs):
        return ax

    @property
    def bounds(self):
        return None, None, None, None


class TimeInterval(Geometry):
    name = Geometry.Types.TIMEINTERVAL

    def __init__(self, start_time=None, end_time=None, geometry=None):
        self.start_time = start_time
        self.end_time = end_time

        if geometry is None:
            geometry = geom_utils.bbox_to_polygon([
                start_time, end_time,
                0, INFINITY])

        super().__init__(geometry=geometry)

    def to_dict(self):
        data = super().to_dict()
        data['start_time'] = self.start_time
        data['end_time'] = self.end_time
        return data

    @property
    def bounds(self):
        return self.start_time, None, self.end_time, None

    @staticmethod
    def _parse_args(arg, kwargs, method):
        if arg is None:
            if 'time' not in kwargs:
                message = (
                    f'Either {method} or time arguments must '
                    f'be supplied to the {method} method.')
                raise ValueError(message)
            time = kwargs['time']

        elif isinstance(arg, (float, int)):
            time = arg

        elif isinstance(arg, (list, float)):
            time = arg[0]

        else:
            message = (
                f'The {method} argument should be a number of a tuple/list')
            raise ValueError(message)

        return time

    def buffer(self, buffer=None, **kwargs):
        time = self._parse_args(buffer, kwargs, 'buffer')
        start_time = self.start_time - time
        end_time = self.end_time + time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def shift(self, shift=None, **kwargs):
        time = self._parse_args(shift, kwargs, 'shift')
        start_time = self.start_time + time
        end_time = self.end_time + time
        return TimeInterval(start_time=start_time, end_time=end_time)

    def dilate(self, dilate=None, center=None, **kwargs):
        time = self._parse_args(dilate, kwargs, 'dilate')

        if center is None:
            center = 0
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
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        ax.axvline(
            self.end_time,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        if kwargs.get('fill', True):
            ax.axvspan(
                self.start_time,
                self.end_time,
                alpha=kwargs.get('alpha', 0.5),
                color=color)

        return ax


class FrequencyInterval(Geometry):
    name = Geometry.Types.FREQUENCYINTERVAL

    def __init__(self, min_freq=None, max_freq=None, geometry=None):
        self.min_freq = min_freq
        self.max_freq = max_freq

        if geometry is None:
            geometry = geom_utils.bbox_to_polygon([
                0, INFINITY,
                min_freq, max_freq
            ])

        super().__init__(geometry=geometry)

    def to_dict(self):
        data = super().to_dict()
        data['min_freq'] = self.min_freq
        data['max_freq'] = self.max_freq
        return data

    @property
    def bounds(self):
        return None, self.min_freq, None, self.max_freq

    @staticmethod
    def _parse_args(arg, kwargs, method):
        if arg is None:
            if 'freq' not in kwargs:
                message = (
                    f'Either {method} or freq arguments must '
                    f'be supplied to the {method} method.')
                raise ValueError(message)
            freq = kwargs['freq']

        elif isinstance(arg, (float, int)):
            freq = arg

        elif isinstance(arg, (list, float)):
            freq = arg[0]

        else:
            message = (
                f'The {method} argument should be a number of a tuple/list')
            raise ValueError(message)

        return freq

    def buffer(self, buffer=None, **kwargs):
        freq = self._parse_args(buffer, kwargs, 'buffer')
        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def shift(self, shift=None, **kwargs):
        freq = self._parse_args(shift, kwargs, 'shift')
        min_freq = self.min_freq + freq
        max_freq = self.max_freq + freq
        return FrequencyInterval(min_freq=min_freq, max_freq=max_freq)

    def dilate(self, dilate=None, center=None, **kwargs):
        freq = self._parse_args(dilate, kwargs, 'dilate')

        if center is None:
            center = 0
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

        ax.axvline(
            self.start_time,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        ax.axvline(
            self.end_time,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        if kwargs.get('fill', True):
            ax.axvspan(
                self.start_time,
                self.end_time,
                alpha=kwargs.get('alpha', 0.5),
                color=color)

        return ax
