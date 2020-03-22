"""Audio Feature module."""
from typing import Optional

from yuntu.core.audio.features.spectrogram import Spectrogram
from yuntu.core.audio.features.spectrogram import PowerSpectrogram
from yuntu.core.audio.features.spectrogram import DecibelSpectrogram


class AudioFeatures:
    """Audio Features class.

    This class is syntactic sugar to access all available features
    that can be derived from an Audio object.
    """

    spectrogram_class = Spectrogram
    power_spectrogram_class = PowerSpectrogram
    db_spectrogram_class = DecibelSpectrogram

    def __init__(self, audio):
        """Construct the Audio Feature object."""
        self.audio = audio

    def get_base_kwargs(self):
        return {
            'annotations': self.audio.annotations.annotations
        }

    def spectrogram(
            self,
            n_fft: Optional[int] = None,
            hop_length: Optional[int] = None,
            window_function: Optional[str] = None,
            lazy: Optional[bool] = False):
        """Get amplitude spectrogram."""
        kwargs = self.get_base_kwargs()
        kwargs['lazy'] = lazy
        if n_fft is not None:
            kwargs['n_fft'] = n_fft

        if hop_length is not None:
            kwargs['hop_length'] = hop_length

        if window_function is not None:
            kwargs['window_function'] = window_function

        return self.spectrogram_class(audio=self.audio, **kwargs)

    def power_spectrogram(
            self,
            n_fft: Optional[int] = None,
            hop_length: Optional[int] = None,
            window_function: Optional[str] = None,
            lazy: Optional[bool] = False):
        """Get power spectrogram."""
        kwargs = self.get_base_kwargs()
        kwargs['lazy'] = lazy
        if n_fft is not None:
            kwargs['n_fft'] = n_fft

        if hop_length is not None:
            kwargs['hop_length'] = hop_length

        if window_function is not None:
            kwargs['window_function'] = window_function

        return self.power_spectrogram_class(audio=self.audio, **kwargs)

    def db_spectrogram(
            self,
            n_fft: Optional[int] = None,
            hop_length: Optional[int] = None,
            window_function: Optional[str] = None,
            lazy: Optional[bool] = False,
            ref: Optional[float] = None,
            amin: Optional[float] = None,
            top_db: Optional[float] = None):
        """Get decibel spectrogram."""
        kwargs = self.get_base_kwargs()
        kwargs['lazy'] = lazy

        if n_fft is not None:
            kwargs['n_fft'] = n_fft

        if hop_length is not None:
            kwargs['hop_length'] = hop_length

        if window_function is not None:
            kwargs['window_function'] = window_function

        if ref is not None:
            kwargs['ref'] = ref

        if amin is not None:
            kwargs['amin'] = amin

        if top_db is not None:
            kwargs['top_db'] = top_db

        return self.db_spectrogram_class(audio=self.audio, **kwargs)
