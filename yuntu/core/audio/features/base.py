"""Feature class module."""
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
        if not isinstance(audio, audio_module.Audio):
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


class TimeFeature(TimeMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            start=None,
            array=None,
            duration=None,
            samplerate=None,
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

        if samplerate is None:
            if audio is not None:
                samplerate = audio.samplerate
            elif array is not None:
                length = array.shape[self.time_axis_index]
                samplerate = (duration - start) / length
            else:
                message = (
                    'If no audio or array is provided a samplerate must '
                    'be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            start=start,
            duration=duration,
            samplerate=samplerate,
            array=array,
            **kwargs)


class FrequencyFeature(FrequencyMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            min_freq=0,
            max_freq=None,
            resolution=None,
            array=None,
            **kwargs):

        if max_freq is None:
            if audio is None:
                message = (
                    'If no audio is provided a maximum frequency must be set')
                raise ValueError(message)
            max_freq = audio.samplerate // 2

        if resolution is None:
            if array is not None:
                length = array.shape[self.frequency_axis_index]
                resolution = (max_freq - min_freq) / length
            else:
                message = (
                    'If no array is provided a resolution must be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            min_freq=min_freq,
            max_freq=max_freq,
            resolution=resolution,
            array=array,
            **kwargs)


class TimeFrequencyFeature(TimeFrequencyMediaMixin, Feature):
    def __init__(
            self,
            audio=None,
            min_freq=0,
            max_freq=None,
            resolution=None,
            start=None,
            duration=None,
            samplerate=None,
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

        if samplerate is None:
            if audio is not None:
                samplerate = audio.samplerate
            elif array is not None:
                length = array.shape[self.time_axis_index]
                samplerate = (duration - start) / length
            else:
                message = (
                    'If no audio or array is provided a samplerate must '
                    'be set')
                raise ValueError(message)

        if max_freq is None:
            if audio is None:
                message = (
                    'If no audio is provided a maximum frequency must be set')
                raise ValueError(message)
            max_freq = audio.samplerate // 2

        if resolution is None:
            if array is not None:
                length = array.shape[self.frequency_axis_index]
                resolution = (max_freq - min_freq) / length
            else:
                message = (
                    'If no array is provided a resolution must be set')
                raise ValueError(message)

        super().__init__(
            audio=audio,
            min_freq=min_freq,
            max_freq=max_freq,
            resolution=resolution,
            start=start,
            duration=duration,
            samplerate=samplerate,
            array=array,
            **kwargs)
