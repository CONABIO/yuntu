import abc

import numpy as np


class Axis(abc.ABC):

    # pylint: disable=unused-argument
    def __init__(
            self,
            start,
            end,
            resolution,
            **kwargs):
        self.start = start
        self.end = end
        self.resolution = resolution

    def to_dict(self):
        return {
            'start': self.start,
            'end': self.end,
            'resolution': self.resolution
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @property
    def period(self):
        return 1 / self.resolution

    @property
    def size(self):
        return int((self.start - self.end) * self.resolution)

    def get_index_from_value(self, value, window=None):
        start = self.get_start(window=window)
        index = int((value - start) * self.resolution)
        return index

    @abc.abstractmethod
    def get_start(self, window=None):
        pass

    @abc.abstractmethod
    def get_end(self, window=None):
        pass

    def get_size(self, window=None):
        start = self.get_start(window=window)
        end = self.get_end(window=window)
        return int((end - start) * self.resolution)

    def get_bins(self, window=None, size=None):
        start = self.get_start(window=window)
        end = self.get_end(window=window)

        if size is None:
            size = self.get_size(window=window)

        return np.linspace(start, end, size)

    def resample(self, resolution):
        data = self.to_dict()
        data['resolution'] = resolution
        return type(self)(**data)

    def copy(self):
        return type(self)(**self.to_dict())


class TimeAxis(Axis):
    def get_start(self, window=None):
        if window is None:
            return self.start

        if window.start is None:
            return self.start

        return window.start

    def get_end(self, window=None):
        if window is None:
            return self.end

        if window.end is None:
            return self.end

        return window.end


class FrequencyAxis(Axis):
    def get_start(self, window=None):
        if window is None:
            return self.start

        if window.min is None:
            return self.start

        return window.min

    def get_end(self, window=None):
        if window is None:
            return self.end

        if window.max is None:
            return self.end

        return window.max
