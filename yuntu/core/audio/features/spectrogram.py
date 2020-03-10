"""Spectrogram class module."""
from collections import namedtuple
import numpy as np

from yuntu.core.audio.features.base import Feature
from yuntu.core.audio.features.spectral import stft

BOXCAR = 'boxcar'
TRIANG = 'triang'
BLACKMAN = 'blackman'
HAMMING = 'hamming'
HANN = 'hann'
BARTLETT = 'bartlett'
FLATTOP = 'flattop'
PARZEN = 'parzen'
BOHMAN = 'bohman'
BLACKMANHARRIS = 'blackmanharris'
NUTTALL = 'nuttall'
BARTHANN = 'barthann'
WINDOW_FUNCTIONS = [
    BOXCAR,
    TRIANG,
    BLACKMAN,
    HAMMING,
    HANN,
    BARTLETT,
    FLATTOP,
    PARZEN,
    BOHMAN,
    BLACKMANHARRIS,
    NUTTALL,
    BARTHANN,
]

N_FFT = 1024
HOP_LENGTH = 512
WINDOW_FUNCTION = HANN

Shape = namedtuple('Shape', ['rows', 'columns'])


class Spectrogram(Feature):
    """Spectrogram class."""

    def __init__(
            self,
            n_fft=N_FFT,
            hop_length=HOP_LENGTH,
            window_function=WINDOW_FUNCTION,
            **kwargs):
        """Construct Spectrogram object."""
        self.n_fft = n_fft
        self.hop_length = hop_length
        self.window_function = window_function
        super().__init__(**kwargs)

    def load(self):
        """Load the spectrogram data."""
        if not self.has_audio() and self.path_exists():
            extension = self.path_ext
            if extension == 'npy':
                try:
                    return np.load(self.path)
                except IOError:
                    message = (
                        'The provided path for this spectrogram object could '
                        f'not be read. (path={self.path})')
                    raise ValueError(message)

            if extension == 'npz':
                try:
                    with np.load(self.path) as data:
                        return data['spectrogram']
                except IOError:
                    message = (
                        'The provided path for this spectrogram object could '
                        f'not be read. (path={self.path})')
                    raise ValueError(message)

            message = (
                'The provided path does not a numpy file extension. '
                f'(extension={extension})')
            raise ValueError(message)

        return np.abs(stft(
            self.audio.data,
            n_fft=self.n_fft,
            hop_length=self.hop_length,
            window=self.window_function))

    def write(self, path):  # pylint: disable=arguments-differ
        """Write the spectrogram matrix into the filesystem."""
        self.path = path
        data = {
            'spectrogram': self.data,
            'duration': self.duration,
            'samplerate': self.samplerate
        }
        np.savez(self.path, **data)

    def rows(self):
        """Get the number of spectrogram rows."""
        return 1 + self.n_fft // 2

    def columns(self):
        """Get the number of spectrogram columns."""
        # If spectrogram is already calculated use the number of columns
        if not self.is_empty():
            return self.data.shape[1]

        # Else, estimate it from the wav length
        wav_length = None
        # Use the length of the wav array if loaded
        if self.has_audio():
            if not self.audio.is_empty():
                wav_length = len(self.audio.data)

        # Otherwise, calculate wav length from duration and samplerate
        if wav_length is None:
            wav_length = self.duration * self.samplerate

        return np.ceil(wav_length / self.hop_length)

    def shape(self) -> Shape:
        """Get spectrogram shape."""
        return Shape(rows=self.rows(), columns=self.columns())

    def get_column_from_time(self, time):
        """Get spectrogram column that corresponds to a given time."""
        if time < 0:
            message = 'Time cannot be negative'
            raise ValueError(message)

        if time > self.duration:
            message = 'Time cannot be greater than the duration of the audio'
            raise ValueError(message)

        return np.floor(self.columns() * time / self.duration)

    def get_row_from_frequency(self, frequency):
        """Get spectrogram row that corresponds to a given frequency."""
        max_frequency = self.samplerate / 2
        if frequency < 0:
            message = 'Frequency cannot be negative'
            raise ValueError(message)

        if frequency > max_frequency:
            message = (
                'Frequency cannot be greater than the nyquist '
                f'frequency: {max_frequency}Hz')
            raise ValueError(message)

        return np.floor(self.rows() * frequency / max_frequency)

    def get_amplitude(self, time, freq):
        """Get spectrogram amplitude value at a given time and frequency."""
        time_index = self.get_column_from_time(time)
        freq_index = self.get_row_from_frequency(freq)
        return self.data[freq_index, time_index]

    @property
    def times(self):
        """Return an array of times.

        The returned array length is the same as the number of columns
        of the spectrogram and indicates the time (in seconds) corresponding
        to each column.
        """
        columns = self.columns()
        return np.linspace(0, self.duration, columns)

    @property
    def frequencies(self):
        """Return an array of frequencies.

        The returned array length is the same as the number of rows
        of the spectrogram and indicates the frequency (in hertz) corresponding
        to each row.
        """
        # If spectrogram is already calculated use the number of columns
        max_frequency = self.samplerate / 2
        rows = 1 + self.n_fft // 2
        return np.linspace(0, max_frequency, rows)

    def plot(self, ax=None, **kwargs):
        """Plot the spectrogram."""
        # pylint: disable=import-outside-toplevel
        import matplotlib.pyplot as plt
        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.pop('figsize', None))

        spectrogram = self.data
        mesh = ax.pcolormesh(
            self.times,
            self.frequencies,
            spectrogram,
            cmap=kwargs.pop('cmap', 'gray'))

        if kwargs.pop('colorbar', False):
            plt.colorbar(mesh, ax=ax)

        return ax

    def __getitem__(self, key):
        """Get spectrogram value."""
        return self.data[key]

    def __iter__(self):
        """Iterate over spectrogram columns."""
        for column in self.data.T:
            yield column

    def iter_rows(self):
        """Iterate over spectrogram rows."""
        for row in self.data:
            yield row

    def iter_cols(self):
        """Iterate over spectrogram columns."""
        for col in self.data.T:
            yield col
