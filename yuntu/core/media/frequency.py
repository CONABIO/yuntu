from typing import Optional
import numpy as np

from yuntu.core.audio.utils import resample
from yuntu.core.media.base import Media
import yuntu.core.windows as windows


class FrequencyMediaMixin:
    frequency_axis = 0
    window_class = windows.FrequencyWindow

    def __init__(self, min_freq=0, max_freq=None, **kwargs):
        self.max_freq = max_freq
        self.min_freq = 0

        if 'window' not in kwargs:
            kwargs['window'] = windows.FrequencyWindow(
                min=min_freq,
                max=max_freq)

        super().__init__(**kwargs)

    def _get_min(self):
        if self.window.min is not None:
            return self.window.min

        return self.min_freq

    def _get_max(self):
        if self.window.max is not None:
            return self.window.max

        return self.max_freq

    def _get_axis_info(self):
        return {
            'max_freq': self.max_freq,
            'min_freq': self.min_freq,
            'resolution': self.resolution,
            **super()._get_axis_info()
        }

    def _has_trivial_window(self):
        if self.window.min is not None:
            min_freq = self._get_min()

            if min_freq != self.window.min:
                return False

        if self.window.max is not None:
            max_freq = self._get_max()

            if max_freq != self.window.max:
                return False

        return super()._has_trivial_window()

    @property
    def df(self):
        return 1 / self.resolution

    @property
    def frequency_size(self):
        if self.is_empty():
            freq_range = self._get_max() - self._get_min()
            return int(freq_range / self.df)
        return self.array.shape[self.frequency_axis]

    def get_index_from_frequency(self, freq):
        """Get index of the media array corresponding to a given frequency."""
        minimum = self._get_min()
        if freq < minimum:
            message = (
                'Frequency less than minimum or window minimum '
                'was requested')
            raise ValueError(message)

        if freq > self._get_max():
            message = (
                'Frequency greater than maximum frequency or window maximum '
                'was requested')
            raise ValueError(message)

        index = int((freq - minimum) / self.df)
        return index

    def get_value(self, freq):
        index = self.get_index_from_frequency(freq)
        return self.array.take(index, axis=self.frequency_axis)

    @property
    def frequencies(self):
        """Get the frequency array.

        This is an array of the same length as the media data array and holds
        the frequency (in hertz) corresponding to each piece of the media
        array.
        """
        minimum = self._get_min()
        maximum = self._get_max()
        return np.linspace(minimum, maximum, self.frequency_size)

    def resample(
            self,
            resolution: int,
            lazy: Optional[bool] = False,
            **kwargs):
        """Get a new FrequencyMedia object with the resampled data."""
        data = self._copy_dict()
        data['lazy'] = lazy
        data['resolution'] = resolution

        if not self.path_exists():
            data = resample(
                self.array,
                self.resolution,
                resolution,
                **kwargs)
            data['array'] = data

        return type(self)(**data)

    def read(self, min_freq=None, max_freq=None):
        """Read a section of the media array.

        Parameters
        ----------
        min_freq: float, optional
            Frequency at which read starts, in hertz. If not provided
            start will be defined as the minimum frequency. Should
            be larger than 0. If a non trivial window is set, the
            provided starting time should be larger that the window
            starting time.
        max_freq: float, optional
            Frequency at which read ends, in hertz. If not provided
            end will be defined as the maximum frequency. Should be
            less than the duration of the audio. If a non trivial
            window is set, the provided ending time should be
            larger that the window ending time.

        Returns
        -------
        np.array
            The media data contained in the demanded frequency limits.

        Raises
        ------
        ValueError
            When min_freq is less than minimum frequency, or max_freq is
            larger than the maximum frequency stored, or min_freq is less
            than 0. If a non trivial window is set, it will also throw an
            error if the requested minimum and maximum frequencies are smaller
            or larger that those set by the window.
        """
        if min_freq is None:
            min_freq = self._get_min()

        if max_freq is None:
            max_freq = self._get_max()

        if min_freq > max_freq:
            message = 'Read min_freq should be less than read max_freq.'
            raise ValueError(message)

        start_index = self.get_index_from_frequency(min_freq)
        end_index = self.get_index_from_frequency(max_freq)

        slice_args = [None for _ in len(self.shape)]
        slice_args[self.frequency_axis] = (start_index, end_index + 1)
        return self.array[slice(*slice_args)]

    def calculate_mask(self, geometry):
        """Return masked 1d array."""
        _, min_freq, _, max_freq = geometry.bounds

        start_index = self.get_index_from_frequency(min_freq)
        end_index = self.get_index_from_frequency(max_freq)

        mask = np.zeros(self.shape)
        mask[start_index: end_index + 1] = 1

        return mask

    def cut(
            self,
            min_freq: float = None,
            max_freq: float = None,
            window: windows.TimeWindow = None,
            lazy=True):
        """Get a window to the media data.

        Parameters
        ----------
        min_freq: float, optional
            Window minimum frequency in Hertz. If not provided
            it will default to the minimum frequency.
        max_freq: float, optional
            Window maximum frequency in Hertz. If not provided
            it will default to the maximum frequency
        window: TimeWindow, optional
            A window object to use for cutting.
        lazy: bool, optional
            Boolean flag that determines if the fragment loads
            its data lazily.

        Returns
        -------
        Media
            The resulting media object with the correct window set.
        """
        current_min = self._get_min()
        current_max = self._get_max()

        if min_freq is None:
            min_freq = (
                window.min
                if window.min is not None
                else min_freq)

        if max_freq is None:
            max_freq = (
                window.max
                if window.max is not None
                else max_freq)

        min_freq = max(min(min_freq, current_max), current_min)
        max_freq = max(min(max_freq, current_max), current_min)

        if max_freq < min_freq:
            message = 'Window is empty'
            raise ValueError(message)

        kwargs_dict = self._copy_dict()
        kwargs_dict['window'] = windows.FrequencyWindow(
            min=min_freq,
            max=max_freq)
        kwargs_dict['lazy'] = lazy

        if not self.is_empty():
            start = self.get_index_from_frequency(min_freq)
            end = self.get_index_from_frequency(max_freq)
            kwargs_dict['array'] = kwargs_dict['array'][slice(start, end)]

        return type(self)(**kwargs_dict)


class FrequencyMedia(FrequencyMediaMixin, Media):
    pass
