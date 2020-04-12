from typing import Optional
import numpy as np

from yuntu.core.audio.utils import resample
from yuntu.core.media.base import Media
import yuntu.core.windows as windows


class TimeMediaMixin:
    time_axis = 0
    window_class = windows.TimeWindow

    def __init__(self, start=0, duration=None, **kwargs):
        self.start = start
        self.duration = duration

        if 'window' not in kwargs:
            kwargs['window'] = windows.TimeWindow(start=start, end=duration)

        super().__init__(**kwargs)

    def _get_start(self):
        if self.window.start is not None:
            return self.window.start

        return self.start

    def _get_end(self):
        if self.window.end is not None:
            return self.window.end

        return self.duration

    def _get_axis_info(self):
        return {
            'start': self.start,
            'duration': self.duration,
            **super()._get_axis_info()
        }

    def _has_trivial_window(self):
        if self.window.start is not None:
            start = self._get_start()

            if start != self.window.start:
                return False

        if self.window.end is not None:
            end = self._get_end()

            if end != self.window.end:
                return False

        return super()._has_trivial_window()

    @property
    def dt(self):
        return 1 / self.resolution

    def get_index_from_time(self, time):
        """Get the index of the media array corresponding to the given time."""
        start = self._get_start()
        if time < start:
            message = (
                'Time earlier that start of recording file or window start '
                'was requested')
            raise ValueError(message)

        if time > self._get_end():
            message = (
                'Time earlier that start of recording file or window start '
                'was requested')
            raise ValueError(message)

        index = int((time - start) / self.dt)
        return index

    def get_value(self, time):
        index = self.get_index_from_time(time)
        return self.array.take(index, axis=self.time_axis)

    def resample(
            self,
            resolution=None,
            samplerate=None,
            lazy: Optional[bool] = False,
            **kwargs):
        """Get a new TemporalMedia object with the resampled data."""
        if samplerate is None and resolution is None:
            message = 'Either resolution or samplerate must be provided'
            raise ValueError(message)

        if resolution is None:
            resolution = samplerate

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

    @property
    def time_size(self):
        if self.is_empty():
            time_range = self._get_end() - self._get_start()
            return int(time_range / self.dt)
        return self.array.shape[self.time_axis]

    @property
    def times(self):
        """Get the time array.

        This is an array of the same length as the wav data array and holds
        the time (in seconds) corresponding to each piece of the wav array.
        """
        start = self._get_start()
        end = self._get_end()

        return np.linspace(start, end, self.time_size)

    def read(self, start=None, end=None):
        """Read a section of the media array.

        Parameters
        ----------
        start: float, optional
            Time at which read starts, in seconds. If not provided
            start will be defined as the recording start. Should
            be larger than 0. If a non trivial window is set, the
            provided starting time should be larger that the window
            starting time.
        end: float, optional
            Time at which read ends, in seconds. If not provided
            end will be defined as the recording end. Should be
            less than the duration of the audio. If a non trivial
            window is set, the provided ending time should be
            larger that the window ending time.

        Returns
        -------
        np.array
            The media data contained in the demanded temporal limits.

        Raises
        ------
        ValueError
            When start is less than end, or end is larger than the
            duration of the audio, or start is less than 0. If a non
            trivial window is set, it will also throw an error if
            the requested starting and ending times are smaller or
            larger that those set by the window.
        """
        if start is None:
            start = self._get_start()

        if end is None:
            end = self._get_end()

        if start > end:
            message = 'Read start should be less than read end.'
            raise ValueError(message)

        start_index = self.get_index_from_time(start)
        end_index = self.get_index_from_time(end)
        return self.array[start_index: end_index + 1]

    def calculate_mask(self, geometry):
        """Return masked 1d array."""
        start, _, end, _ = geometry.bounds

        start_index = self.get_index_from_time(start)
        end_index = self.get_index_from_time(end)

        mask = np.zeros(self.shape)
        mask[start_index: end_index + 1] = 1

        return mask

    def cut(
            self,
            start_time: float = None,
            end_time: float = None,
            window: windows.TimeWindow = None,
            lazy=True):
        """Get a window to the media data.

        Parameters
        ----------
        start: float, optional
            Window starting time in seconds. If not provided
            it will default to the beggining of the recording.
        end: float, optional
            Window ending time in seconds. If not provided
            it will default to the duration of the recording.
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
        current_start = self._get_start()
        current_end = self._get_end()

        if start_time is None:
            start_time = (
                window.start
                if window.start is not None
                else current_start)

        if end_time is None:
            end_time = (
                window.end
                if window.end is not None
                else current_end)

        start_time = max(min(start_time, current_end), current_start)
        end_time = max(min(end_time, current_end), current_start)

        if end_time < start_time:
            message = 'Window is empty'
            raise ValueError(message)

        kwargs_dict = self._copy_dict()
        kwargs_dict['window'] = windows.TimeWindow(
            start=start_time,
            end=end_time)
        kwargs_dict['lazy'] = lazy

        if not self.is_empty():
            start = self.get_index_from_time(start_time)
            end = self.get_index_from_time(end_time)
            kwargs_dict['array'] = kwargs_dict['array'][slice(start, end)]

        return type(self)(**kwargs_dict)


class TimeMedia(TimeMediaMixin, Media):
    pass
