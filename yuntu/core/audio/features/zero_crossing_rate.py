"""Zero crossing class module."""
import librosa
import numpy as np

import yuntu.core.audio.audio as audio_mod
from yuntu.core.audio.features.base import TimeFeature


THRESHOLD = 1e-10
FRAME_LENGTH = 2048
HOP_LENGTH = 512


class ZeroCrossingRate(TimeFeature):
    def __init__(
            self,
            audio=None,
            array=None,
            threshold=THRESHOLD,
            ref_magnitude=None,
            frame_length=FRAME_LENGTH,
            hop_length=HOP_LENGTH,
            duration=None,
            resolution=None,
            time_axis=None,
            **kwargs):

        self.threshold = threshold
        self.ref_magnitude = ref_magnitude
        self.frame_length = frame_length
        self.hop_length = hop_length

        if audio is not None and not isinstance(audio, audio_mod.Audio):
            audio = audio_mod.Audio.from_dict(audio)

        if duration is None:
            if audio is None:
                message = (
                    'If no audio is provided a duration must be set')
                raise ValueError(message)
            duration = audio.duration

        if resolution is None:
            if array is not None:
                length = len(array)
            elif audio is not None:
                length = 1 + (len(audio) - frame_length) // hop_length
            else:
                message = (
                    'If no audio or array is provided a samplerate must be '
                    'set')
                raise ValueError(message)

            resolution = length / duration

        super().__init__(
            audio=audio,
            duration=duration,
            resolution=resolution,
            array=array,
            time_axis=time_axis,
            **kwargs)

    def compute(self):
        zero_crossings = librosa.core.zero_crossings(
            self.audio,
            threshold=self.threshold,
            ref_magnitude=self.ref_magnitude)

        frames = librosa.util.frame(
            zero_crossings,
            frame_length=self.frame_length,
            hop_length=self.hop_length)

        frame_duration = self.frame_length / self.audio.samplerate
        crossings_per_frame = frames.sum(axis=-2)
        return crossings_per_frame / (2 * frame_duration)

    def to_dict(self):
        return {
            'threshold': self.threshold,
            'ref_magnitude': self.ref_magnitude,
            'frame_length': self.frame_length,
            'hop_length': self.hop_length,
            **super().to_dict(),
        }

    def write(self, path=None):
        pass

    def plot(self, ax=None, **kwargs):
        # pylint: disable=import-outside-toplevel
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', None))

        masked = np.ma.masked_less_equal(self.array, kwargs.get('min_freq', 0))
        if kwargs.get('max_freq', 0):
            masked = np.ma.masked_greater_equal(self.array, kwargs.get('max_freq'))

        times = self.times
        ax.plot(
            times,
            masked,
            color=kwargs.get('color', 'black'),
            linestyle=kwargs.get('linestyle', 'dotted'),
            linewidth=kwargs.get('linewidth', 1))
        ax.set_xlim(times[0], times[-1])

        ylim_bottom = kwargs.get('ylim_bottom', 0)
        ax.set_ylim(bottom=ylim_bottom)

        ylim_top = kwargs.get('ylim_top', None)
        if ylim_top is None:
            if self.has_audio():
                ylim_top = self.audio.samplerate / 2
            else:
                ylim_top = self.array.max()
        ax.set_ylim(top=ylim_top)

        xlabel = kwargs.get('xlabel', True)
        if xlabel:
            if not isinstance(xlabel, str):
                xlabel = 'Time (s)'
            ax.set_xlabel(xlabel)

        ylabel = kwargs.get('ylabel', True)
        if ylabel:
            if not isinstance(ylabel, str):
                ylabel = 'Frequency (Hz)'
            ax.set_ylabel(ylabel)

        title = kwargs.get('title', True)
        if title:
            if not isinstance(title, str):
                title = 'Zero Crossing Rate'
            ax.set_title(title)

        return ax
