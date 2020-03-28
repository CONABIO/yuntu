from uuid import uuid4
from abc import ABC
from abc import abstractmethod
from enum import Enum

import numpy as np

from yuntu.core.windows import TimeWindow
from yuntu.core.windows import TimeFrequencyWindow
from yuntu.core.windows import FrequencyWindow
from yuntu.core.annotation.labels import Labels
import yuntu.core.geometry.base as geom
import yuntu.core.geometry.utils as geom_utils


INFINITY = 10e+9


class Annotation(ABC):
    window_class = TimeWindow
    name = None

    class Types(Enum):
        WEAK = 'WeakAnnotation'
        INTERVAL = 'IntervalAnnotation'
        FREQUENCY_INTERVAL = 'FrequencyIntervalAnnotation'
        BBOX = 'BBoxAnnotation'
        LINESTRING = 'LineStringAnnotation'
        POLYGON = 'PolygonAnnotation'

    def __init__(
            self,
            target=None,
            labels=None,
            id=None,
            metadata=None,
            geometry=None,
            **kwargs):

        if id is None:
            id = str(uuid4())
        self.id = id

        if labels is None:
            labels = Labels([])

        if not isinstance(labels, Labels):
            labels = Labels(labels)

        self.target = target
        self.labels = labels
        self.metadata = metadata
        self.geometry = geometry

    def has_label(self, key, mode='all'):
        if key is None:
            return True

        if not isinstance(key, (tuple, list)):
            return key in self.labels

        if mode == 'all':
            for subkey in key:
                if subkey not in self.labels:
                    return False
            return True

        if mode == 'any':
            for subkey in key:
                if subkey in self.labels:
                    return True
            return False

        message = 'Mode must be "all" or "any"'
        raise ValueError(message)

    @property
    def type(self):
        return self.name

    def iter_labels(self):
        return iter(self.labels)

    def get_label(self, key):
        return self.labels.get(key)

    def get_label_value(self, key):
        return self.labels.get_value(key)

    def get_window(self):
        kwargs = self.get_window_kwargs()
        return self.window_class(**kwargs)

    @abstractmethod
    def get_window_kwargs(self):
        pass

    def buffer(self, buffer=None, **kwargs):
        data = self.to_dict()
        data['labels'] = self.labels
        data['geometry'] = self.geometry.buffer(buffer=buffer, **kwargs)

        if self.target is not None:
            data['target'] = self.target

        return type(self)(**data)

    def add_label(
            self,
            value=None,
            key=None,
            type=None,
            data=None,
            label=None):
        self.labels.add(
            value=value,
            key=key,
            type=type,
            data=data,
            label=label)

    def to_dict(self):
        data = {
            'id': self.id,
            'labels': self.labels.to_dict(),
            'type': self.type.value,
            'geometry': self.geometry.to_dict()
        }

        if self.target is not None:
            data['target'] = repr(self.target)

        if self.metadata is not None:
            data['metadata'] = self.metadata

        return data

    @classmethod
    def from_dict(cls, data, target=None):
        data = data.copy()
        annotation_type = data.pop('type')
        data['labels'] = Labels.from_dict(data['labels'])
        data['geometry'] = geom.Geometry.from_dict(data['geometry'])

        if target is not None:
            data['target'] = target

        if annotation_type == Annotation.Types.WEAK.value:
            return WeakAnnotation(**data)

        if annotation_type == Annotation.Types.INTERVAL.value:
            return IntervalAnnotation(**data)

        if annotation_type == Annotation.Types.FREQUENCY_INTERVAL.value:
            return FrequencyIntervalAnnotation(**data)

        if annotation_type == Annotation.Types.BBOX.value:
            return BBoxAnnotation(**data)

        if annotation_type == Annotation.Types.LINESTRING.value:
            return LineStringAnnotation(**data)

        if annotation_type == Annotation.Types.POLYGON.value:
            return PolygonAnnotation(**data)

        message = 'Annotation is of unknown type.'
        raise ValueError(message)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax = self.geometry.plot(ax=ax, **kwargs)

        if kwargs.get('label', True):
            self.plot_labels(ax, **kwargs)

        return ax

    def plot_labels(self, ax, **kwargs):
        from matplotlib import transforms

        label = self.labels.get_text_repr(kwargs.get('key', None))
        left, bottom, right, top = self.geometry.bounds

        xcoord_position = kwargs.get('label_xposition', 'center')
        if left is None or right is None:
            xtransform = ax.transAxes

            if not isinstance(xcoord_position, (int, float)):
                xcoord = 0.5
        else:
            xtransform = ax.transData

            if isinstance(xcoord_position, str):
                if xcoord_position == 'left':
                    xcoord = left

                elif xcoord_position == 'right':
                    xcoord = right

                elif xcoord_position == 'center':
                    xcoord = (left + right) / 2

                else:
                    message = (
                        'label_xposition can only be a float, '
                        'or "left"/"right"/"center"')
                    raise ValueError(message)

            if isinstance(xcoord_position, float):
                xcoord = left + (right - left) * xcoord_position

        ycoord_position = kwargs.get('label_yposition', 'center')
        if bottom is None or top is None:
            ytransform = ax.transAxes

            if not isinstance(ycoord_position, (int, float)):
                ycoord = 0.5
        else:
            ytransform = ax.transData

            if isinstance(ycoord_position, str):
                if ycoord_position == 'top':
                    ycoord = top

                elif ycoord_position == 'bottom':
                    ycoord = bottom

                elif ycoord_position == 'center':
                    ycoord = (top + bottom) / 2

                else:
                    message = (
                        'ycoord_position can only be a float, '
                        'or "top"/"bottom"/"center"')
                    raise ValueError(message)

            if isinstance(ycoord_position, float):
                ycoord = bottom + (top - bottom) * ycoord_position

        trans = transforms.blended_transform_factory(xtransform, ytransform)
        ax.text(
            xcoord,
            ycoord,
            label,
            transform=trans,
            ha=kwargs.get('label_ha', 'center'))

    def cut(self, other):
        return other.cut(self.get_window())


class WeakAnnotation(Annotation):
    name = Annotation.Types.WEAK

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.geometry = geom.Weak()

    def get_window_kwargs(self):
        return {}


class IntervalAnnotation(Annotation):
    name = Annotation.Types.INTERVAL

    def __init__(self, start_time=None, end_time=None, **kwargs):
        if 'geometry' not in kwargs:
            kwargs['geometry'] = geom.TimeInterval(
                start_time=start_time,
                end_time=end_time)

        super().__init__(**kwargs)

    @property
    def start_time(self):
        return self.geometry.start_time

    @property
    def end_time(self):
        return self.geometry.start_time

    def get_window_kwargs(self):
        return {
            'start': self.start_time,
            'end': self.end_time
        }

    def center_buffer(self, buffer):
        center = (self.start_time + self.end_time) / 2
        start = center - buffer
        end = center + buffer
        IntervalAnnotation(
            target=self.target,
            start_time=start,
            end_time=end,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_weak(self):
        return WeakAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)
    #
    # def plot(self, ax=None, **kwargs):
    #     import matplotlib.pyplot as plt
    #     import matplotlib.transforms as transforms
    #
    #     if ax is None:
    #         _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))
    #
    #     color = kwargs.get('color', None)
    #     if color is None:
    #         color = next(ax._get_lines.prop_cycler)['color']
    #
    #     ax.axvline(
    #         self.start_time,
    #         linewidth=kwargs.get('linewidth', 1),
    #         linestyle=kwargs.get('linestyle', '--'),
    #         color=color)
    #
    #     ax.axvline(
    #         self.end_time,
    #         linewidth=kwargs.get('linewidth', 1),
    #         linestyle=kwargs.get('linestyle', '--'),
    #         color=color,
    #         label=str(self.labels))
    #
    #     if kwargs.get('fill', True):
    #         ax.axvspan(
    #             self.start_time,
    #             self.end_time,
    #             alpha=kwargs.get('alpha', 0.5),
    #             color=color)
    #
    #     if kwargs.get('label', True):
    #         label = self.labels.get_text_repr(kwargs.get('key', None))
    #
    #         start = self.start_time
    #         end = self.end_time
    #         position = kwargs.get('label_xposition', 'center')
    #         yposition = kwargs.get('label_yposition', 0.5)
    #
    #         trans = transforms.blended_transform_factory(
    #             ax.transData, ax.transAxes)
    #
    #         if position == 'center':
    #             xcoord = (start + end) / 2
    #         elif position == 'left':
    #             xcoord = start
    #         else:
    #             xcoord = end
    #
    #         ax.text(xcoord, yposition, label, transform=trans)
    #     return ax


class FrequencyIntervalAnnotation(Annotation):
    window_class = FrequencyWindow
    name = Annotation.Types.FREQUENCY_INTERVAL

    def __init__(self, min_freq=None, max_freq=None, **kwargs):
        super().__init__(**kwargs)
        self.min_freq = min_freq
        self.max_freq = max_freq
        self.geometry = geom_utils.bbox_to_polygon([
            0, INFINITY,
            self.min_freq, self.max_freq])

    def buffer(self, buffer):
        if isinstance(buffer, (float, int)):
            freq = buffer

        elif isinstance(buffer, (list, float)):
            freq = buffer[1]

        else:
            message = 'The buffer argument should be a number of a tuple/list'
            raise ValueError(message)

        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq

        return FrequencyIntervalAnnotation(
            target=self.target,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def center_buffer(self, buffer):
        center = (self.min_freq + self.max_freq) / 2
        min_freq = center - buffer
        max_freq = center + buffer
        FrequencyIntervalAnnotation(
            target=self.target,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def get_window_kwargs(self):
        return {
            'min': self.min_freq,
            'max': self.max_freq
        }

    def to_dict(self):
        data = super().to_dict()
        data['min_freq'] = self.min_freq
        data['max_freq'] = self.max_freq
        return data

    def to_weak(self):
        return WeakAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt
        import matplotlib.transforms as transforms

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        color = kwargs.get('color', None)
        if color is None:
            color = next(ax._get_lines.prop_cycler)['color']

        line = ax.axhline(
            self.min_freq,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        ax.axhline(
            self.max_freq,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=color)

        if kwargs.get('fill', True):
            ax.axhspan(
                self.min_freq,
                self.max_freq,
                alpha=kwargs.get('alpha', 0.5),
                color=color)

        if kwargs.get('label', True):
            label = self.labels.get_text_repr(kwargs.get('key', None))

            min_freq = self.min_freq
            max_freq = self.max_freq
            position = kwargs.get('label_yposition', 'center')
            xposition = kwargs.get('label_xposition', 0.5)

            trans = transforms.blended_transform_factory(
                ax.transAxes, ax.transData)

            if position == 'center':
                ycoord = (min_freq + max_freq) / 2
            elif position == 'left':
                ycoord = min_freq
            else:
                ycoord = max_freq

            ax.text(xposition, ycoord, label, transform=trans)

        return ax


class BBoxAnnotation(Annotation):
    window_class = TimeFrequencyWindow
    name = Annotation.Types.BBOX

    def __init__(
            self,
            start_time=None,
            end_time=None,
            min_freq=None,
            max_freq=None,
            **kwargs):
        super().__init__(**kwargs)

        if self.geometry is None:
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

            self.geometry = geom_utils.bbox_to_polygon([
                start_time, end_time,
                min_freq, max_freq])

        if not geom_utils.validate_geometry(self.geometry, geom_utils.Geometries.POLYGON):
            message = (
                'The Bounding Box Annotation geometry is not a '
                'polygon.')
            raise ValueError(message)

        start_time, min_freq, end_time, max_freq = self.geometry.bounds
        self.start_time = start_time
        self.end_time = end_time
        self.min_freq = min_freq
        self.max_freq = max_freq

    def __repr__(self):
        args = [
            f'start_time={self.start_time}',
            f'end_time={self.end_time}',
            f'max_freq={self.max_freq}',
            f'min_freq={self.min_freq}',
            f'labels={repr(self.labels)}',
        ]
        args_string = ', '.join(args)
        return f'BBoxAnnotation({args_string})'

    def get_window_kwargs(self):
        return {
            'start': self.start_time,
            'end': self.end_time,
            'max': self.max_freq,
            'min': self.min_freq
        }

    def to_dict(self):
        data = super().to_dict()
        data['start_time'] = self.start_time
        data['end_time'] = self.end_time
        data['min_freq'] = self.min_freq
        data['max_freq'] = self.max_freq
        return data

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        xcoords, ycoords = self.geometry.exterior.xy
        lineplot, = ax.plot(
            xcoords,
            ycoords,
            linewidth=kwargs.get('linewidth', 1),
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
                linewidth=0)

        if kwargs.get('label', True):
            label = self.labels.get_text_repr(kwargs.get('key', None))

            min_x, min_y, max_x, max_y = self.geometry.bounds
            position = kwargs.get('label_position', 'center')
            if position == 'center':
                xcoord = (min_x + max_x) / 2
                ycoord = (min_y + max_y) / 2
            elif position == 'top':
                xcoord = (min_x + max_x) / 2
                ycoord = max_y
            else:
                xcoord = max_x
                ycoord = (min_y + max_y) / 2

            ax.text(xcoord, ycoord, label)

        return ax

    def to_weak(self):
        return WeakAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_frequency_interval(self):
        _, min_freq, _, max_freq = self.geometry.bounds
        return FrequencyIntervalAnnotation(
            target=self.target,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_time_interval(self):
        start_time, _, end_time, _ = self.geometry.bounds
        return IntervalAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def buffer(self, buffer):
        if isinstance(buffer, (int, float)):
            buffer = [buffer, buffer]

        if len(buffer) == 1:
            buffer.append(0)

        time, freq = buffer

        if time is None:
            time = 0

        if freq is None:
            freq = 0

        start_time = self.start_time - time
        end_time = self.end_time + time
        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq

        return BBoxAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def center_buffer(self, buffer):
        time_buffer, freq_buffer = buffer
        time_center = (self.start_time + self.end_time) / 2
        freq_center = (self.min_freq + self.max_freq) / 2
        start_time = time_center - time_buffer
        end_time = time_center + time_buffer
        min_freq = freq_center - freq_buffer
        max_freq = freq_center + freq_buffer

        BBoxAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)


class LineStringAnnotation(Annotation):
    window_class = TimeFrequencyWindow
    name = Annotation.Types.LINESTRING

    def __init__(
            self,
            vertices=None,
            **kwargs):
        super().__init__(**kwargs)

        if vertices is None:
            vertices = []

        if self.geometry is None:
            self.geometry = geom_utils.linestring_geometry(vertices)

        if not geom_utils.validate_geometry(self.geometry, geom_utils.Geometries.LINESTRING):
            message = (
                'The Line String Annotation geometry is not a '
                'linestring.')
            raise ValueError(message)

        self.vertices = list(self.geometry.coords)

    def __repr__(self):
        args = [
            f'vertices={self.vertices}',
            f'labels={repr(self.labels)}',
        ]
        args_string = ', '.join(args)
        return f'LineStringAnnotation({args_string})'

    def to_dict(self):
        data = super().to_dict()
        data['vertices'] = self.vertices
        return data

    def interpolate(self, s, normalized=True):
        return self.geometry.interpolate(s, normalized=normalized).coords[0]

    def resample(self, num_samples):
        vertices = [
            self.interpolate(param)
            for param in np.linspace(0, 1, num_samples, endpoint=True)
        ]
        return LineStringAnnotation(
            target=self.target,
            labels=self.labels,
            metadata=self.metadata,
            id=self.id,
            vertices=vertices)

    def get_window_kwargs(self):
        start, min_freq, end, max_freq = self.geometry.bounds
        return {
            'start': start,
            'end': end,
            'max': max_freq,
            'min': min_freq
        }

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        xcoords, ycoords = self.geometry.xy
        lineplot, = ax.plot(
            xcoords,
            ycoords,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', None),
            linestyle=kwargs.get('linestyle', '--'),
        )

        color = lineplot.get_color()

        if kwargs.get('label', True):
            label = self.labels.get_text_repr(kwargs.get('key', None))

            position = kwargs.get('label_position', 'center')
            if position == 'center':
                middle = len(xcoords) // 2
                xcoord = xcoords[middle]
                ycoord = ycoords[middle]
            elif position == 'start':
                xcoord = xcoords[0]
                ycoord = ycoords[0]
            else:
                xcoord = xcoords[-1]
                ycoord = ycoords[-1]

            ax.text(xcoord, ycoord, label)

        if kwargs.get('scatter', False):
            ax.scatter(
                xcoords,
                ycoords,
                color=color,
                s=kwargs.get('size', 5))

        return ax

    def to_weak(self):
        return WeakAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_frequency_interval(self):
        _, min_freq, _, max_freq = self.geometry.bounds
        return FrequencyIntervalAnnotation(
            target=self.target,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_time_interval(self):
        start_time, _, end_time, _ = self.geometry.bounds
        return IntervalAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_bbox(self):
        start_time, min_freq, end_time, max_freq = self.geometry.bounds
        return BBoxAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def buffer(self, buffer):
        if isinstance(buffer, (int, float)):
            buffer = [buffer, buffer]

        geometry = geom_utils.buffer_geometry(self.geometry, buffer=buffer)
        return PolygonAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata,
            geometry=geometry)


class PolygonAnnotation(Annotation):
    window_class = TimeFrequencyWindow
    name = Annotation.Types.POLYGON

    def __init__(
            self,
            shell=None,
            holes=None,
            **kwargs):
        super().__init__(**kwargs)

        if shell is None:
            shell = []

        if holes is None:
            holes = []

        if self.geometry is None:
            self.geometry = geom_utils.polygon_geometry(shell, holes)

        if not geom_utils.validate_geometry(self.geometry, geom_utils.Geometries.POLYGON):
            message = (
                'The Polygon Annotation geometry is not a '
                'polygon.')
            raise ValueError(message)

        self.shell = list(self.geometry.exterior.coords)
        self.holes = [
            list(interior.coords)
            for interior in self.geometry.interiors]

    def __repr__(self):
        args = [
            f'shell={self.shell}',
            f'labels={repr(self.labels)}',
        ]

        if self.holes:
            args.append(f'holes={self.holes}')

        args_string = ', '.join(args)
        return f'PolygonAnnotation({args_string})'

    def to_dict(self):
        data = super().to_dict()
        data['shell'] = self.shell

        if self.holes:
            data['holes'] = self.holes

        return data

    def to_weak(self):
        return WeakAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_frequency_interval(self):
        _, min_freq, _, max_freq = self.geometry.bounds
        return FrequencyIntervalAnnotation(
            target=self.target,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_time_interval(self):
        start_time, _, end_time, _ = self.geometry.bounds
        return IntervalAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def to_bbox(self):
        start_time, min_freq, end_time, max_freq = self.geometry.bounds
        return BBoxAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            min_freq=min_freq,
            max_freq=max_freq,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def get_window_kwargs(self):
        start, min_freq, end, max_freq = self.geometry.bounds
        return {
            'start': start,
            'end': end,
            'max': max_freq,
            'min': min_freq
        }

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
                linewidth=kwargs.get('linewidth', 1),
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

        if kwargs.get('label', True):
            label = self.labels.get_text_repr(kwargs.get('key', None))
            xcoords, ycoords = self.geometry.exterior.xy
            ax.text(np.mean(xcoords), np.mean(ycoords), label)

        return ax

    def buffer(self, buffer):
        if isinstance(buffer, (int, float)):
            buffer = [buffer, buffer]

        geometry = geom_utils.buffer_geometry(self.geometry, buffer=buffer)
        return PolygonAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata,
            geometry=geometry)
