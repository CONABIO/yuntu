from typing import Optional
from collections import namedtuple

import numpy as np
from scipy.interpolate import interp2d
from scipy.interpolate import RectBivariateSpline

import yuntu.core.windows as windows
from yuntu.core.media.base import Media
from yuntu.core.media.time import TimeMediaMixin
from yuntu.core.media.frequency import FrequencyMediaMixin
import yuntu.core.geometry.utils as geom_utils


TimeFreqResolution = namedtuple('TimeFreqResolution', 'time freq')


class TimeFrequencyMediaMixin(TimeMediaMixin, FrequencyMediaMixin):
    frequency_axis = 0
    time_axis = 1
    window_class = windows.TimeFrequencyWindow

    def __init__(
            self,
            start=None,
            duration=None,
            min_freq=None,
            max_freq=None,
            resolution=None,
            **kwargs):

        if not isinstance(resolution, TimeFreqResolution):
            time, freq = resolution
            resolution = TimeFreqResolution(time, freq)

        if 'window' not in kwargs:
            kwargs['window'] = windows.TimeFrequencyWindow(
                start=start,
                end=duration,
                min=min_freq,
                max=max_freq)

        super().__init__(
            start=start,
            resolution=resolution,
            duration=duration,
            min_freq=min_freq,
            max_freq=max_freq,
            **kwargs)

    @property
    def dt(self):
        return 1 / self.resolution.time

    @property
    def df(self):
        return 1 / self.resolution.freq

    def get_value(self, time: float, freq: float) -> float:
        """Get media value at a given time and frequency.

        Parameters
        ----------
        time: float
            Time in seconds.
        freq: float
            Frequency in hertz.

        Returns
        -------
        float
            The value of the spectrogram at the desired time and frequency.
        """
        time_index = self.get_index_from_time(time)
        freq_index = self.get_index_from_frequency(freq)

        if self.time_axis > self.frequency_axis:
            first_axis = self.time_axis
            first_index = time_index

            second_axis = self.frequency_axis
            second_index = freq_index
        else:
            first_axis = self.time_axis
            first_index = time_index

            second_axis = self.frequency_axis
            second_index = freq_index

        result = self.array.take(first_index, axis=first_axis)
        return result.take(second_index, axis=second_axis)

    # pylint: disable=arguments-differ
    def read(
            self,
            start_time=None,
            end_time=None,
            min_freq=None,
            max_freq=None):
        if min_freq is None:
            min_freq = self._get_min()

        if max_freq is None:
            max_freq = self._get_max()

        if min_freq > max_freq:
            message = 'Read min_freq should be less than read max_freq.'
            raise ValueError(message)

        if start_time is None:
            start_time = self._get_start()

        if end_time is None:
            end_time = self._get_end()

        if start_time > end_time:
            message = 'Read start_time should be less than read end_time.'
            raise ValueError(message)

        start_freq_index = self.get_index_from_frequency(min_freq)
        end_freq_index = self.get_index_from_frequency(max_freq)

        start_time_index = self.get_index_from_time(start_time)
        end_time_index = self.get_index_from_time(end_time)

        slice_args = [slice(None, None, None) for _ in len(self.shape)]
        slice_args[self.frequency_axis] = slice(
            start_freq_index,
            end_freq_index + 1)
        slice_args[self.time_axis] = slice(
            start_time_index,
            end_time_index + 1)
        return self.array[tuple(slice_args)]

    def cut(
            self,
            window: Optional[windows.TimeFrequencyWindow] = None,
            start_time: Optional[float] = None,
            end_time: Optional[float] = None,
            max_freq: Optional[float] = None,
            min_freq: Optional[float] = None,
            lazy: Optional[bool] = False,
            **kwargs):
        current_start = self._get_start()
        current_end = self._get_end()
        current_min = self._get_min()
        current_max = self._get_max()

        if start_time is None:
            try:
                start_time = max(min(window.start, current_end), current_start)
            except (AttributeError, TypeError):
                pass

        if end_time is None:
            try:
                end_time = max(min(window.end, current_end), current_start)
            except (AttributeError, TypeError):
                pass

        if min_freq is None:
            try:
                min_freq = max(min(window.min, current_max), current_min)
            except (AttributeError, TypeError):
                pass

        if max_freq is None:
            try:
                max_freq = max(min(window.max, current_max), current_min)
            except (AttributeError, TypeError):
                pass

        try:
            if start_time > end_time or min_freq > max_freq:
                raise ValueError('Cut is empty')
        except TypeError:
            pass

        kwargs = self._copy_dict()
        kwargs['lazy'] = lazy
        kwargs['window'] = windows.TimeFrequencyWindow(
            start=start_time,
            end=end_time,
            min=min_freq,
            max=max_freq)

        if not self.is_empty():
            if start_time is None:
                start_index = None
            else:
                start_index = self.get_index_from_time(start_time)

            if end_time is None:
                end_index = None
            else:
                end_index = self.get_index_from_time(end_time)

            if min_freq is None:
                min_index = None
            else:
                min_index = self.get_index_from_frequency(min_freq)

            if max_freq is None:
                max_index = None
            else:
                max_index = self.get_index_from_frequency(max_freq)

            row_slice = slice(min_index, max_index)
            col_slice = slice(start_index, end_index)
            data = self.array[row_slice, col_slice]
            kwargs['array'] = data.copy()

        return type(self)(**kwargs)

    def resample(
            self,
            samplerate=None,
            resolution=None,
            lazy: Optional[bool] = False,
            kind: str = 'linear',
            **kwargs):
        """Get a new FrequencyMedia object with the resampled data."""
        if resolution is None:
            resolution = self.resolution

        if not isinstance(resolution, TimeFreqResolution):
            resolution = TimeFreqResolution(*resolution)

        if samplerate is not None:
            resolution = TimeFreqResolution(
                time=samplerate,
                freq=resolution.freq)

        data = self._copy_dict()
        data['lazy'] = lazy
        data['resolution'] = resolution

        if not self.path_exists():
            if self.ndim != 2:
                message = (
                    'Media elements with more than 2 dimensions cannot be'
                    ' resampled')
                raise ValueError(message)

            start_time = self._get_start()
            end_time = self._get_end()

            min_freq = self._get_min()
            max_freq = self._get_max()

            new_time_bins = int((end_time - start_time) * resolution.time)
            new_times = np.linspace(start_time, end_time, new_time_bins)

            new_freq_bins = int((max_freq - min_freq) * resolution.freq)
            new_freqs = np.linspace(min_freq, max_freq, new_freq_bins)

            if self.time_axis == 1:
                xcoord = self.times
                ycoord = self.frequencies

                newxcoord = new_times
                newycoord = new_freqs
            else:
                xcoord = self.frequencies
                ycoord = self.times

                newxcoord = new_freqs
                newycoord = new_times

            if kind == 'linear':
                interp = interp2d(
                    xcoord,
                    ycoord,
                    self.array,
                    **kwargs)
            else:
                interp = RectBivariateSpline(
                    xcoord,
                    ycoord,
                    self.array,
                    **kwargs)

            data['array'] = interp(newxcoord, newycoord)

        return type(self)(**data)

    def get_aggr_value(
            self,
            time=None,
            freq=None,
            buffer=None,
            bins=0,
            window=None,
            geometry=None,
            aggr_func=np.mean):
        if bins is None:
            bins = 0

        if bins != 0 and buffer is not None:
            message = 'Bins and buffer arguments are mutually exclusive.'
            raise ValueError(message)

        if time is not None and freq is not None:
            if buffer is None:
                values = geom_utils.point_neighbourhood(
                    self.array,
                    [time, freq],
                    bins,
                    self.get_index_from_time,
                    self.get_index_from_frequency)
                return aggr_func(values)

            geometry = geom_utils.point_geometry(time, freq)

        if window is not None:
            if buffer is not None:
                window = window.buffer(buffer)

            values = self.cut(window=window).array
            return aggr_func(values)

        if geometry is None:
            message = (
                'Either time and frequency, a window, or a geometry '
                'should be supplied.')
            raise ValueError(message)

        if buffer is not None:
            geometry = geom_utils.buffer_geometry(geometry, buffer)

        values = geom_utils.geometry_neighbourhood(
            self.array,
            geometry,
            bins,
            self.get_index_from_time,
            self.get_index_from_frequency)
        return aggr_func(values)

    def calculate_mask(self, geometry):
        return geom_utils.geometry_to_mask(
            geometry,
            self.array.shape,
            transformX=self.get_index_from_time,
            transformY=self.get_index_from_frequency)


class TimeFrequencyMedia(TimeFrequencyMediaMixin, Media):
    pass
