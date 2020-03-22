from uuid import uuid4
from abc import ABC
from abc import abstractmethod

from yuntu.core.windows import TimeWindow
from yuntu.core.windows import TimeFrequencyWindow
from yuntu.core.windows import FrequencyWindow
from yuntu.core.annotation.labels import Labels
from yuntu.core.atlas.geometry import linestring_geometry, \
                                      polygon_geometry, \
                                      validate_geometry, \
                                      bbox_to_polygon, \
                                      buffer_geometry


INFINITY = 10e+9

class Annotation(ABC):
    window_class = TimeWindow

    def __init__(
            self,
            target=None,
            labels=None,
            id=None,
            metadata=None,
            geometry=None):

        if id is None:
            id = uuid4()
        self.id = id

        if labels is None:
            labels = Labels([])

        if not isinstance(labels, Labels):
            labels = Labels(labels)

        self.target = target
        self.labels = labels
        self.metadata = metadata
        self.geometry = geometry

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
    def buffer(self):
        pass

    @abstractmethod
    def get_window_kwargs(self):
        pass

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
        }

        if self.target is not None:
            data['target'] = repr(self.target)

        if self.geometry is not None:
            data['geometry'] = self.geometry.wkt

        if self.metadata is not None:
            data['metadata'] = self.metadata

        return data

    @classmethod
    def from_dict(cls, data, target=None):
        annotation_id = data.get('id', None)

        geometry = data.get('geometry', None)
        if geometry is not None:
            geometry = geom_from_wkt(geometry)

        metadata = data.get('metadata', None)
        labels = Labels.from_dict(data['labels'])

        return cls(
            target,
            labels,
            id=annotation_id,
            metadata=metadata,
            geometry=geometry)

    def cut(self, other):
        return other.cut(self.get_window())


class WeakAnnotation(Annotation):
    def get_window_kwargs(self):
        return {}

    def buffer(self):
        return WeakAnnotation(**self.to_dict())


class IntervalAnnotation(Annotation):
    def __init__(self, start_time=None, end_time=None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.end_time = end_time
        self.geometry = bbox_to_polygon([self.start_time, self.end_time,
                                         0, INFINITY])

    def buffer(self, time):
        start_time = self.start_time - time
        end_time = self.end_time + time

        return IntervalAnnotation(
            target=self.target,
            start_time=start_time,
            end_time=end_time,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata)

    def get_window_kwargs(self):
        return {
            'start': self.start_time,
            'end': self.end_time
        }

    def to_dict(self):
        data = super().to_dict()
        data['start_time'] = self.start_time
        data['end_time'] = self.end_time
        return data

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax.axvline(
            self.start_time,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', 'red'))

        ax.axvline(
            self.end_time,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', 'red'))

        if kwargs.get('fill', True):
            ax.axvspan(
                self.start_time,
                self.end_time,
                alpha=kwargs.get('alpha', 0.5),
                color=kwargs.get('color', 'red'))

        return ax


class FrequencyIntervalAnnotation(Annotation):
    window_class = FrequencyWindow

    def __init__(self, min_freq=None, max_freq=None, **kwargs):
        super().__init__(**kwargs)
        self.min_freq = min_freq
        self.max_freq = max_freq
        self.geometry = bbox_to_polygon([0, INFINITY,
                                         self.min_freq, self.max_freq])

    def buffer(self, freq):
        min_freq = self.min_freq - freq
        max_freq = self.max_freq + freq

        return FrequencyIntervalAnnotation(
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

    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 10)))

        ax.axhline(
            self.min_freq,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', 'red'))

        ax.axhline(
            self.max_freq,
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', '--'),
            color=kwargs.get('color', 'red'))

        if kwargs.get('fill', True):
            ax.axhspan(
                self.min_freq,
                self.max_freq,
                alpha=kwargs.get('alpha', 0.5),
                color=kwargs.get('color', 'red'))

        return ax


class BBoxAnnotation(Annotation):
    window_class = TimeFrequencyWindow

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

            self.geometry = bbox_to_polygon((start_time, end_time,
                                             min_freq, max_freq))

        if not validate_geometry(self.geometry, 'Polygon'):
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

        plt.plot(
            *self.geometry.exterior.xy,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', 'red'),
            linestyle=kwargs.get('linestyle', '--'),
        )

        return ax

    def buffer(self, time=None, freq=None):
        if time is None and freq is None:
            message = (
                'Either time and freq (or both) must be provided to '
                'buffer method.')
            raise ValueError(message)

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


class LineStringAnnotation(Annotation):
    window_class = TimeFrequencyWindow

    def __init__(
            self,
            vertices=None,
            **kwargs):
        super().__init__(**kwargs)

        if vertices is None:
            vertices = []

        if self.geometry is None:
            self.geometry = linestring_geometry(vertices)

        if validate_geometry(self.geometry, 'LineString'):
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

        plt.plot(
            *self.geometry.xy,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', 'red'),
            linestyle=kwargs.get('linestyle', '--'),
        )

        return ax

    def buffer(self, time=None, freq=None):
        if time is None or freq is None:
            message = 'Both time and freq must be set in buffer method.'
            raise ValueError(message)
        geometry = buffer_geometry(self.geometry, buffer=(time, freq))
        return PolygonAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata,
            geometry=geometry)


class PolygonAnnotation(Annotation):
    window_class = TimeFrequencyWindow

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
            self.geometry = polygon_geometry(shell, holes)

        if validate_geometry(self.geometry, 'Polygon'):
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

        ax.plot(
            *self.geometry.exterior.xy,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', 'red'),
            linestyle=kwargs.get('linestyle', '--'),
        )

        for interior in self.geometry.interiors:
            ax.plot(
                *interior.xy,
                linewidth=kwargs.get('linewidth', 1),
                color=kwargs.get('color', 'red'),
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
                color=kwargs.get('color', 'red'),
                alpha=kwargs.get('alpha', 0.5),
                linewidth=0)

        return ax

    def buffer(self, time=None, freq=None):
        if time is None or freq is None:
            message = 'Both time and freq must be set in buffer method.'
            raise ValueError(message)
        geometry = buffer_geometry(self.geometry, buffer=(time, freq))
        return PolygonAnnotation(
            target=self.target,
            labels=self.labels,
            id=self.id,
            metadata=self.metadata,
            geometry=geometry)
