from typing import Optional

import numpy as np
from scipy.interpolate import interp2d
from scipy.interpolate import RectBivariateSpline

import yuntu.core.windows as windows
from yuntu.core.geometry import base as geom
from yuntu.core.annotation import annotation
from yuntu.core.media import masked
from yuntu.core.media.base import Media
from yuntu.core.media.time import TimeMediaMixin
from yuntu.core.media.time import TimeItem
from yuntu.core.media.frequency import FrequencyMediaMixin
from yuntu.core.media.frequency import FrequencyItem
import yuntu.core.geometry.utils as geom_utils


class TimeItemWithFrequencies(FrequencyMediaMixin, TimeItem):
    pass


class FrequencyItemWithTime(TimeMediaMixin, FrequencyItem):
    pass


class TimeFrequencyMediaMixin(TimeMediaMixin, FrequencyMediaMixin):
    frequency_axis_index = 0
    time_axis_index = 1

    window_class = windows.TimeFrequencyWindow
    time_item_class = TimeItemWithFrequencies
    frequency_item_class = FrequencyItemWithTime

    plot_xlabel = 'Time (s)'
    plot_ylabel = 'Frequency (Hz)'

    def __init__(
            self,
            start=0,
            duration=None,
            time_resolution=None,
            time_axis=None,
            min_freq=None,
            max_freq=None,
            freq_resolution=None,
            frequency_axis=None,
            **kwargs):
        if time_axis is None:
            time_axis = self.time_axis_class(
                start=start,
                end=duration,
                resolution=time_resolution)

        if not isinstance(time_axis, self.time_axis_class):
            time_axis = self.time_axis_class.from_dict(time_axis)

        if frequency_axis is None:
            frequency_axis = self.frequency_axis_class(
                start=min_freq,
                end=max_freq,
                resolution=freq_resolution)

        if not isinstance(frequency_axis, self.frequency_axis_class):
            frequency_axis = self.frequency_axis_class.from_dict(frequency_axis) # noqa

        if 'window' not in kwargs:
            kwargs['window'] = windows.TimeFrequencyWindow(
                start=time_axis.start,
                end=time_axis.end,
                min=frequency_axis.start,
                max=frequency_axis.end)

        super().__init__(
            start=start,
            frequency_axis=frequency_axis,
            time_axis=time_axis,
            **kwargs)

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

        if self.time_axis_index > self.frequency_axis_index:
            first_axis = self.time_axis_index
            first_index = time_index

            second_axis = self.frequency_axis_index
            second_index = freq_index
        else:
            first_axis = self.frequency_axis_index
            first_index = freq_index

            second_axis = self.time_axis_index
            second_index = time_index

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

        slices = self._build_slices(
            start_time_index,
            end_time_index + 1,
            start_freq_index,
            end_freq_index + 1)
        return self.array[slices]

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

            slices = self._build_slices(
                start_index,
                end_index,
                min_index,
                max_index)
            data = self.array[slices]
            kwargs['array'] = data.copy()

        return type(self)(**kwargs)

    def resample(
            self,
            time_resolution=None,
            freq_resolution=None,
            lazy: Optional[bool] = False,
            kind: str = 'linear',
            **kwargs):
        """Get a new FrequencyMedia object with the resampled data."""
        if time_resolution is None:
            time_resolution = self.time_axis.resolution

        if freq_resolution is None:
            freq_resolution = self.frequency_axis.resolution

        data = self._copy_dict()
        data['lazy'] = lazy
        new_time_axis = self.time_axis.resample(time_resolution)
        data['time_axis'] = new_time_axis

        new_freq_axis = self.frequency_axis.resample(freq_resolution)
        data['frequency_axis'] = new_freq_axis

        if not lazy:
            if self.ndim != 2:
                message = (
                    'Media elements with more than 2 dimensions cannot be'
                    ' resampled')
                raise ValueError(message)

            new_times = new_time_axis.get_bins(window=self.window)
            new_freqs = new_freq_axis.get_bins(window=self.window)

            if self.time_axis_index == 1:
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

    def get_freq_item_kwargs(self, freq):
        return {
            'window': self.window.copy(),
            'time_axis': self.time_axis
        }

    def get_time_item_kwargs(self, freq):
        return {
            'window': self.window.copy(),
            'frequency_axis': self.frequency_axis
        }

    def get_aggr_value(
            self,
            time=None,
            freq=None,
            buffer=None,
            bins=None,
            window=None,
            geometry=None,
            aggr_func=np.mean):
        if bins is not None and buffer is not None:
            message = 'Bins and buffer arguments are mutually exclusive.'
            raise ValueError(message)

        if buffer is None and bins is not None:
            if not isinstance(bins, (list, tuple)):
                bins = [bins, bins]

            time_buffer = self.time_axis.resolution * bins[0]
            freq_buffer = self.frequency_axis.resolution * bins[1]
            buffer = [time_buffer, freq_buffer]

        if window is not None:
            if not isinstance(window, windows.Window):
                window = windows.Window.from_dict(window)

            if buffer is not None:
                window = window.buffer(buffer)

            values = self.cut(window=window).array
            return aggr_func(values)

        if time is not None and freq is not None:
            geometry = geom.Point(time, freq)

        if geometry is None:
            message = (
                'Either time and frequency, a window, or a geometry '
                'should be supplied.')
            raise ValueError(message)

        if buffer is not None:
            geometry = geometry.buffer(buffer)

        mask = self.to_mask(geometry)
        return aggr_func(self.array[mask.array])

    def to_mask(self, geometry, lazy=False):
        if isinstance(geometry, (annotation.Annotation, windows.Window)):
            geometry = geometry.geometry

        if not isinstance(geometry, geom.Geometry):
            geometry = geom.Geometry.from_geometry(geometry)

        intersected = geometry.intersection(self.window)

        return self.mask_class(
            media=self,
            geometry=intersected,
            lazy=lazy,
            time_axis=self.time_axis,
            frequency_axis=self.frequency_axis)

    # pylint: disable=arguments-differ
    def _build_slices(self, start_time, end_time, min_freq, max_freq):
        slice_args = [slice(None, None, None) for _ in self.shape]
        slice_args[self.frequency_axis_index] = slice(start_time, end_time)
        slice_args[self.time_axis_index] = slice(min_freq, max_freq)
        return tuple(slice_args)


@masked.masks(TimeFrequencyMediaMixin)
class TimeFrequencyMaskedMedia(TimeFrequencyMediaMixin, masked.MaskedMedia):
    plot_title = 'Time Frequency Masked Object'

    def plot(self, ax=None, **kwargs):
        ax = super().plot(ax=ax, **kwargs)

        if kwargs.get('mask', True):
            ax.pcolormesh(
                self.times,
                self.frequencies,
                self.array,
                cmap=kwargs.get('cmap', 'gray'),
                alpha=kwargs.get('alpha', 1.0))

        return ax

    def load(self, path=None):
        return geom_utils.geometry_to_mask(
            self.geometry.geometry,
            self.media.array.shape,
            transformX=self.media.get_index_from_time,
            transformY=self.media.get_index_from_frequency)


class TimeFrequencyMedia(TimeFrequencyMediaMixin, Media):
    pass
