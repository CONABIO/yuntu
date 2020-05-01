"""Feature class module."""
from abc import abstractmethod
import os

import numpy as np
import yuntu.core.audio.audio as audio_module
from yuntu.core.media.base import Media
from yuntu.core.media.time import TimeMediaMixin
from yuntu.core.media.frequency import FrequencyMediaMixin
from yuntu.core.media.time_frequency import TimeFrequencyMediaMixin


# pylint: disable=abstract-method
class Feature(Media):
    """Feature base class.

    This is the base class for all audio features. A feature contains
    information extracted from the audio data.
    """

    def __init__(
            self,
            audio=None,
            array=None,
            path: str = None,
            lazy: bool = False,
            **kwargs):
        """Construct a feature."""
        if audio is not None and not isinstance(audio, audio_module.Audio):
            audio = audio_module.Audio.from_dict(audio)

        self.audio = audio
        super().__init__(path=path, lazy=lazy, array=array, **kwargs)

    def _copy_dict(self, **kwargs):
        return {
            'audio': self.audio,
            **super()._copy_dict(**kwargs),
        }

    def to_dict(self):
        data = super().to_dict()

        if self.has_audio():
            data['audio'] = self.audio.to_dict()

        return data

    def has_audio(self):
        """Return if this feature is linked to an Audio instance."""
        if not hasattr(self, 'audio'):
            return False

        return self.audio is not None

    def load_from_path(self, path=None):
        if path is None:
            path = self.path

        extension = os.path.splitext(path)[1]
        if extension == 'npy':
            try:
                return np.load(self.path)
            except IOError:
                message = (
                    'The provided path for this feature object could '
                    f'not be read. (path={self.path})')
                raise ValueError(message)

        if extension == 'npz':
            try:
                with np.load(self.path) as data:
                    return data[type(self).__name__]
            except IOError:
                message = (
                    'The provided path for this feature object could '
                    f'not be read. (path={self.path})')
                raise ValueError(message)

        message = (
            'The provided path does not have a numpy file extension. '
            f'(extension={extension})')
        raise ValueError(message)

    @abstractmethod
    def compute(self):
        pass

    def load(self, path=None):
        if not self.has_audio():
            if not self.path_exists(path):
                message = (
                    'The provided path to feature file does not exist.')
                raise ValueError(message)

            return self.load_from_path(path)

        return self.compute()


class TimeFeature(TimeMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            start=None,
            array=None,
            duration=None,
            time_frequency=None,
            **kwargs):

        if start is None:
            if audio is None:
                start = 0
            else:
                start = audio.time_axis.start

        if duration is None:
            if audio is None:
                message = (
                    'If no audio is provided a duration must be set')
                raise ValueError(message)
            duration = audio.duration

        if time_frequency is None:
            if audio is not None:
                time_frequency = audio.samplerate
            elif array is not None:
                length = array.shape[self.time_axis_index]
                time_frequency = (duration - start) / length
            else:
                message = (
                    'If no audio or array is provided a time_frequency must '
                    'be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            start=start,
            duration=duration,
            time_frequency=time_frequency,
            array=array,
            **kwargs)


class FrequencyFeature(FrequencyMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            min_freq=0,
            max_freq=None,
            freq_resolution=None,
            array=None,
            **kwargs):

        if max_freq is None:
            if audio is None:
                message = (
                    'If no audio is provided a maximum frequency must be set')
                raise ValueError(message)
            max_freq = audio.samplerate // 2

        if freq_resolution is None:
            if array is not None:
                length = array.shape[self.frequency_axis_index]
                freq_resolution = (max_freq - min_freq) / length
            else:
                message = (
                    'If no array is provided a freq_resolution must be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            min_freq=min_freq,
            max_freq=max_freq,
            freq_resolution=freq_resolution,
            array=array,
            **kwargs)


class TimeFrequencyFeature(TimeFrequencyMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            min_freq=0,
            max_freq=None,
            freq_resolution=None,
            start=None,
            duration=None,
            time_resolution=None,
            array=None,
            **kwargs):

        if start is None:
            if audio is None:
                start = 0
            else:
                start = audio.time_axis.start

        if duration is None:
            if audio is None:
                message = (
                    'If no audio is provided a duration must be set')
                raise ValueError(message)
            duration = audio.duration

        if time_resolution is None:
            if audio is not None:
                time_resolution = audio.samplerate
            elif array is not None:
                length = array.shape[self.time_axis_index]
                time_resolution = (duration - start) / length
            else:
                message = (
                    'If no audio or array is provided a time_resolution must '
                    'be set')
                raise ValueError(message)

        if max_freq is None:
            if audio is None:
                message = (
                    'If no audio is provided a maximum frequency must be set')
                raise ValueError(message)
            max_freq = audio.samplerate // 2

        if freq_resolution is None:
            if array is not None:
                length = array.shape[self.frequency_axis_index]
                freq_resolution = (max_freq - min_freq) / length
            else:
                message = (
                    'If no array is provided a freq_resolution must be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            min_freq=min_freq,
            max_freq=max_freq,
            freq_resolution=freq_resolution,
            start=start,
            duration=duration,
            time_resolution=time_resolution,
            array=array,
            **kwargs)
