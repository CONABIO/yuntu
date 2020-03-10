"""Spectrogram class module."""
from typing import Optional
from collections import namedtuple
import numpy as np
from librosa.core import amplitude_to_db
from librosa.core import power_to_db

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

    units = 'amplitude'

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

    def calculate(self):
        """Calculate spectrogram from audio data.

        Uses the spectrogram instance configurations for stft
        calculation.

        Returns
        -------
        np.array
            Calculated spectrogram.
        """
        return np.abs(stft(
            self.audio.data,
            n_fft=self.n_fft,
            hop_length=self.hop_length,
            window=self.window_function))

    def load(self):
        """Load the spectrogram data.

        Will try to load from file if no audio object was provided at
        creation.

        Returns
        -------
        np.array
            The calculated spectrogram

        Raises
        ------
        ValueError
            - If no audio was given and the path provided does no exists.
            - If the file at path is not a numpy file.
            - If the numpy file at path is corrupted.
        """
        if not self.has_audio():
            if not self.path_exists():
                message = (
                    'The provided path to spectrogram file does not exist.')
                raise ValueError(message)

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

        return self.calculate()

    def write(self, path):  # pylint: disable=arguments-differ
        """Write the spectrogram matrix into the filesystem."""
        self.path = path
        data = {
            'spectrogram': self.data,
            'duration': self.duration,
            'samplerate': self.samplerate
        }

        if self.has_audio():
            if self.audio.exists():
                data['audio_path'] = self.audio.path

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

    def get_column_from_time(self, time: float) -> int:
        """Get spectrogram column that corresponds to a given time.

        Parameters
        ----------
        time: float
            Time in seconds

        Returns
        -------
        int
            The spectrogram column corresponding to the provided time.

        Raises
        ------
        ValueError
            If time is negative or larger than the audio duration.
        """
        if time < 0:
            message = 'Time cannot be negative'
            raise ValueError(message)

        if time > self.duration:
            message = 'Time cannot be greater than the duration of the audio'
            raise ValueError(message)

        return np.floor(self.columns() * time / self.duration)

    def get_row_from_frequency(self, frequency: float) -> int:
        """Get spectrogram row that corresponds to a given frequency.

        Parameters
        ----------
        frequency: float
            Frequency in hertz

        Returns
        -------
        int
            The spectrogram row corresponding to the provided frequency.

        Raises
        ------
        ValueError
            If frequency is negative or larger than the nyquist frequency.
        """
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

    def get_amplitude(self, time: float, freq: float) -> float:
        """Get spectrogram amplitude value at a given time and frequency.

        Parameters
        ----------
        time: float
            Time in seconds.
        freq: float
            Frequency in hertz.

        Returns
        -------
        float
            The amplitude of the spectrogram at the desired time and frequency.
        """
        time_index = self.get_column_from_time(time)
        freq_index = self.get_row_from_frequency(freq)
        return self.data[freq_index, time_index]

    @property
    def times(self) -> np.array:
        """Return an array of times.

        The returned array length is the same as the number of columns
        of the spectrogram and indicates the time (in seconds) corresponding
        to each column.

        Returns
        -------
        np.array
            Array of times.
        """
        columns = self.columns()
        return np.linspace(0, self.duration, columns)

    @property
    def frequencies(self) -> np.array:
        """Return an array of frequencies.

        The returned array length is the same as the number of rows
        of the spectrogram and indicates the frequency (in hertz) corresponding
        to each row.

        Returns
        -------
        np.array
            Array of frequencies.
        """
        # If spectrogram is already calculated use the number of columns
        max_frequency = self.samplerate / 2
        rows = 1 + self.n_fft // 2
        return np.linspace(0, max_frequency, rows)

    def plot(self, ax=None, **kwargs):
        """Plot the spectrogram.

        Notes
        -----
        Will create a new figure if no axis (ax) was provided.

        Arguments
        ---------
        figsize: tuple, optional
            Figure size in inches
        cmap: str, optional
            Colormap to use for spectrogram plotting
        colorbar: bool, optional
            Flag indicating whether to draw a colorbar.
        set_xlabel: bool, optional
            Flag indicating wheter to set the x-axis label of
            the provided axis.
        xlabel: str, optional
            The label to use for the x-axis. Defaults to "Time(s)".
        set_ylabel: bool, optional
            Flag indicating wheter to set the y-axis label of
            the provided axis.
        ylabel: str, optional
            The label to use for the y-axis. Defaults to "Frequency(Hz)".
        set_title: bool, optional
            Flag indicating wheter to set the title of
            the provided axis.
        title: str, optional
            The title to use. Defaults to "Spectrogram".

        Returns
        -------
        matplotlib.Axes
            The axis into which the spectrogram was plotted.
        """
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

        if kwargs.pop('set_xlabel', False):
            ax.set_xlabel(kwargs.pop('xlabel', 'Time (s)'))

        if kwargs.pop('set_ylabel', False):
            ax.set_ylabel(kwargs.pop('ylabel', 'Frequency (Hz)'))

        if kwargs.pop('set_title', False):
            ax.set_title(kwargs.pop('title', f'Spectrogram ({self.units})'))

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

    def power(self, lazy=False):
        """Get power spectrogram from spec."""
        kwargs = {
            'n_fft': self.n_fft,
            'hop_length': self.hop_length,
            'window_function': self.window_function,
            'duration': self.duration,
            'samplerate': self.samplerate,
            'lazy': lazy
        }

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['data'] = self.data ** 2

        return PowerSpectrogram(**kwargs)

    def db(
            self,
            lazy: Optional[bool] = False,
            ref: Optional[float] = None,
            amin: Optional[float] = None,
            top_db: Optional[float] = None):
        """Get decibel spectrogram from spec."""
        kwargs = {
            'n_fft': self.n_fft,
            'hop_length': self.hop_length,
            'window_function': self.window_function,
            'duration': self.duration,
            'samplerate': self.samplerate,
            'lazy': lazy
        }

        if ref is not None:
            kwargs['ref'] = ref

        if amin is not None:
            kwargs['amin'] = amin

        if top_db is not None:
            kwargs['top_db'] = top_db

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['data'] = amplitude_to_db(self.data)

        return DecibelSpectrogram(**kwargs)


class PowerSpectrogram(Spectrogram):
    """Power spectrogram class."""

    units = 'power'

    def calculate(self):
        """Calculate spectrogram from audio data."""
        spectrogram = super().calculate()
        return spectrogram**2

    def db(
            self,
            lazy: Optional[bool] = False,
            ref: Optional[float] = None,
            amin: Optional[float] = None,
            top_db: Optional[float] = None):
        """Get decibel spectrogram from power spec."""
        kwargs = {
            'n_fft': self.n_fft,
            'hop_length': self.hop_length,
            'window_function': self.window_function,
            'duration': self.duration,
            'samplerate': self.samplerate,
            'lazy': lazy
        }

        if ref is not None:
            kwargs['ref'] = ref

        if amin is not None:
            kwargs['amin'] = amin

        if top_db is not None:
            kwargs['top_db'] = top_db

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['data'] = power_to_db(self.data)

        return DecibelSpectrogram(**kwargs)


class DecibelSpectrogram(Spectrogram):
    """Decibel spectrogram class."""

    units = 'db'
    ref = 1.0
    amin = 1e-05
    top_db = 80.0

    def __init__(self, ref=None, amin=None, top_db=None, **kwargs):
        """Construct a decibel spectrogram."""
        if ref is not None:
            self.ref = ref

        if amin is not None:
            self.amin = amin

        if top_db is not None:
            self.top_db = top_db

        super().__init__(**kwargs)

    def calculate(self):
        """Calculate spectrogram from audio data."""
        spectrogram = super().calculate()
        return amplitude_to_db(
            spectrogram,
            ref=self.ref,
            amin=self.amin,
            top_db=self.top_db)
