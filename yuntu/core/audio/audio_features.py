"""Audio Feature module."""
from yuntu.core.audio.features.spectrogram import Spectrogram


class AudioFeatures:
    """Audio Features class.

    This class is syntactic sugar to access all available features
    that can be derived from an Audio object.
    """

    spectrogram_class = Spectrogram

    def __init__(self, audio):
        """Construct the Audio Feature object."""
        self.audio = audio

    def spectrogram(
            self,
            n_fft=None,
            hop_length=None,
            window_function=None,
            lazy=False):
        """Get audio spectrogram."""
        kwargs = {'lazy': lazy}
        if n_fft is not None:
            kwargs['n_fft'] = n_fft

        if hop_length is not None:
            kwargs['hop_length'] = hop_length

        if window_function is not None:
            kwargs['window_function'] = window_function

        return Spectrogram(audio=self.audio, **kwargs)
