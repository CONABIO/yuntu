"""Spectrogram class module."""
from typing import Optional
import os
from collections import namedtuple
from collections import OrderedDict

import numpy as np
from librosa.core import amplitude_to_db
from librosa.core import power_to_db

from yuntu.logging import logger
from yuntu.core.annotation.annotated_object import AnnotatedObject
from yuntu.core.windows import TimeFrequencyWindow
from yuntu.core.audio.features.base import Feature
from yuntu.core.audio.features.spectral import stft
from yuntu.core.atlas.geometry import geometry_to_mask
from yuntu.core.atlas.geometry import point_neighbourhood
from yuntu.core.atlas.geometry import point_geometry
from yuntu.core.atlas.geometry import geometry_neighbourhood
from yuntu.core.atlas.geometry import buffer_geometry


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


class Spectrogram(AnnotatedObject, Feature):
    """Spectrogram class."""

    units = 'amplitude'
    window_class = TimeFrequencyWindow

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

    def __repr__(self):
        data = OrderedDict()

        if self.n_fft != N_FFT:
            data['n_fft'] = self.n_fft

        if self.hop_length != HOP_LENGTH:
            data['hop_length'] = self.hop_length

        if self.window_function != WINDOW_FUNCTION:
            data['window_function'] = self.window_function

        has_path = self.path_exists()
        if has_path:
            data['path'] = repr(self.path)

        has_audio = self.has_audio()
        if not has_path and has_audio:
            data['audio'] = repr(self.audio)

        if not has_audio and not has_path:
            data['array'] = repr(self.array)

        if not self.window.is_trivial():
            data['window'] = repr(self.window)

        class_name = type(self).__name__
        args = [f'{key}={value}' for key, value in data.items()]
        args_string = ', '.join(args)

        return f'{class_name}({args_string})'

    def _get_start_time(self):
        default = 0
        if not hasattr(self.window, 'start'):
            return default

        if self.window.start is None:
            return default

        return self.window.start

    def _get_end_time(self):
        default = self.duration
        if not hasattr(self.window, 'end'):
            return default

        if self.window.end is None:
            return default

        return self.window.end

    def _get_max_freq(self):
        default = self.samplerate / 2
        if not hasattr(self.window, 'max'):
            return default

        if self.window.max is None:
            return default

        return self.window.max

    def _get_min_freq(self):
        default = 0
        if not hasattr(self.window, 'min'):
            return default

        if self.window.min is None:
            return default

        return self.window.min

    def calculate(self):
        """Calculate spectrogram from audio data.

        Uses the spectrogram instance configurations for stft
        calculation.

        Returns
        -------
        np.array
            Calculated spectrogram.
        """
        if not self.window.is_trivial():
            start = self._get_start_time()
            end = self._get_end_time()
            array = self.audio.cut(start=start, end=end).array
        else:
            array = self.audio.array

        result = np.abs(stft(
            array,
            n_fft=self.n_fft,
            hop_length=self.hop_length,
            window=self.window_function))

        if self.window.is_trivial():
            return result

        max_freq = self._get_max_freq()
        min_freq = self._get_min_freq()
        nyquist = self.samplerate / 2
        rows = 1 + self.n_fft // 2
        max_index = int(rows * (max_freq / nyquist))
        min_index = int(rows * (min_freq / nyquist))
        return result[slice(min_index, max_index)]

    def load(self):
        """Load the spectrogram array.

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
                'The provided path does not have a numpy file extension. '
                f'(extension={extension})')
            raise ValueError(message)

        return self.calculate()

    def write(self, path):  # pylint: disable=arguments-differ
        """Write the spectrogram matrix into the filesystem."""
        self.path = path
        data = {
            'spectrogram': self.array,
            'duration': self.duration,
            'samplerate': self.samplerate,
        }

        if self.has_audio():
            if self.audio.exists():
                data['audio_path'] = self.audio.path

        if not self.window.is_trivial():
            window_data = {
                f'window_{key}': value
                for key, value in self.window.to_dict()
                if value is not None
            }
            data.update(window_data)

        np.savez(self.path, **data)

    def rows(self):
        """Get the number of spectrogram rows."""
        if not self.is_empty():
            return self.array.shape[0]

        max_rows = 1 + self.n_fft // 2
        nyquist = self.samplerate / 2
        min_freq = self._get_min_freq()
        max_freq = self._get_max_freq()
        return int(max_rows * ((max_freq - min_freq) / nyquist))

    def columns(self):
        """Get the number of spectrogram columns."""
        # If spectrogram is already calculated use the number of columns
        if not self.is_empty():
            return self.array.shape[1]

        start = self._get_start_time()
        end = self._get_end_time()
        duration = end - start
        wav_length = duration * self.samplerate
        return int(np.ceil(wav_length / self.hop_length))

    @property
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
        """
        start_time = self._get_start_time()
        if time < start_time:
            time = start_time

        end_time = self._get_end_time()
        if time > end_time:
            time = end_time

        duration = end_time - start_time
        return int(np.floor(self.columns() * ((time - start_time) / duration)))

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
        """
        min_freq = self._get_min_freq()
        if frequency < min_freq:
            frequency = min_freq

        max_freq = self._get_max_freq()
        if frequency > max_freq:
            frequency = max_freq

        return int(np.floor(self.rows() * ((frequency - min_freq) / max_freq)))

    def get_value(self, time: float, freq: float) -> float:
        """Get spectrogram value at a given time and frequency.

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
        time_index = self.get_column_from_time(time)
        freq_index = self.get_row_from_frequency(freq)

        return self.array[freq_index, time_index]

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
                values = point_neighbourhood(self.array,
                                             [time, freq],
                                             bins,
                                             self.get_column_from_time,
                                             self.get_row_from_frequency)
                return aggr_func(values)

            geometry = point_geometry(time, freq)

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
            geometry = buffer_geometry(geometry, buffer)

        values = geometry_neighbourhood(self.array,
                                        geometry,
                                        bins,
                                        self.get_column_from_time,
                                        self.get_row_from_frequency)
        return aggr_func(values)

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
        start = self._get_start_time()
        end = self._get_end_time()
        return np.linspace(start, end, columns)

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
        rows = self.rows()
        min_freq = self._get_min_freq()
        max_freq = self._get_max_freq()
        return np.linspace(min_freq, max_freq, rows)

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
        w_xlabel: bool, optional
            Flag indicating wheter to set the x-axis label of
            the provided axis.
        xlabel: str, optional
            The label to use for the x-axis. Defaults to "Time(s)".
        w_ylabel: bool, optional
            Flag indicating wheter to set the y-axis label of
            the provided axis.
        ylabel: str, optional
            The label to use for the y-axis. Defaults to "Frequency(Hz)".
        w_title: bool, optional
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

        spectrogram = self.array
        mesh = ax.pcolormesh(
            self.times,
            self.frequencies,
            spectrogram,
            cmap=kwargs.pop('cmap', 'gray'),
            alpha=kwargs.pop('alpha', 1.0))

        if kwargs.pop('colorbar', False):
            plt.colorbar(mesh, ax=ax)

        if kwargs.pop('w_xlabel', False):
            ax.set_xlabel(kwargs.pop('xlabel', 'Time (s)'))

        if kwargs.pop('w_ylabel', False):
            ax.set_ylabel(kwargs.pop('ylabel', 'Frequency (Hz)'))

        if kwargs.pop('w_title', False):
            ax.set_title(kwargs.pop('title', f'Spectrogram ({self.units})'))

        if kwargs.pop('w_window', False):
            min_freq = self._get_min_freq()
            max_freq = self._get_max_freq()
            start_time = self._get_start_time()
            end_time = self._get_end_time()
            line_x, line_y = zip(*[
                [start_time, min_freq],
                [end_time, min_freq],
                [end_time, max_freq],
                [start_time, max_freq],
                [start_time, min_freq],
            ])
            ax.plot(
                line_x,
                line_y,
                color=kwargs.pop('window_color', 'red'),
                linewidth=kwargs.pop('window_linewidth', 3))

        return ax

    def iter_rows(self):
        """Iterate over spectrogram rows."""
        for row in self.array:
            yield row

    def iter_cols(self):
        """Iterate over spectrogram columns."""
        for col in self.array.T:
            yield col

    def power(self, lazy=False):
        """Get power spectrogram from spec."""
        kwargs = self.to_dict()
        kwargs['window'] = self.window

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['array'] = self.array ** 2

        return PowerSpectrogram(**kwargs)

    # pylint: disable=invalid-name
    def db(
            self,
            lazy: Optional[bool] = False,
            ref: Optional[float] = None,
            amin: Optional[float] = None,
            top_db: Optional[float] = None):
        """Get decibel spectrogram from spec."""
        kwargs = self.to_dict()
        kwargs['window'] = self.window

        if ref is not None:
            kwargs['ref'] = ref

        if amin is not None:
            kwargs['amin'] = amin

        if top_db is not None:
            kwargs['top_db'] = top_db

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['array'] = amplitude_to_db(self.array)

        return DecibelSpectrogram(**kwargs)

    def cut(
            self,
            window: Optional[TimeFrequencyWindow] = None,
            start_time: Optional[float] = None,
            end_time: Optional[float] = None,
            max_freq: Optional[float] = None,
            min_freq: Optional[float] = None,
            lazy: Optional[bool] = False,
            **kwargs):
        current_start = self._get_start_time()
        current_end = self._get_end_time()
        current_min = self._get_min_freq()
        current_max = self._get_max_freq()

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

        kwargs = self.to_dict()
        kwargs['window'] = TimeFrequencyWindow(
            start=start_time,
            end=end_time,
            min=min_freq,
            max=max_freq)
        kwargs['lazy'] = lazy

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty():
            if start_time is None:
                start_index = None
            else:
                start_index = self.get_column_from_time(start_time)

            if end_time is None:
                end_index = None
            else:
                end_index = self.get_column_from_time(end_time)

            if min_freq is None:
                min_index = None
            else:
                min_index = self.get_row_from_frequency(min_freq)

            if max_freq is None:
                max_index = None
            else:
                max_index = self.get_row_from_frequency(max_freq)

            row_slice = slice(min_index, max_index)
            col_slice = slice(start_index, end_index)
            data = self.array[row_slice, col_slice]
            kwargs['array'] = data.copy()

        return type(self)(**kwargs)

    def to_mask(self, geometry):
        if geometry is None:
            return np.ones_like(self.array)
        return geometry_to_mask(
            geometry,
            self.array.shape,
            transformX=self.get_column_from_time,
            transformY=self.get_row_from_frequency)

    # pylint: disable=arguments-differ
    def to_dict(self, absolute_path=True):
        """Return spectrogram metadata."""
        data = {
            'units': self.units,
            'n_fft': self.n_fft,
            'hop_length': self.hop_length,
            'window_function': self.window_function,
            'window': self.window.to_dict(),
            'duration': self.duration,
            'samplerate': self.samplerate,
        }

        if self.path_exists():
            if absolute_path:
                data['path'] = os.path.abspath(self.path)
            else:
                data['path'] = self.path

            return data

        if self.has_audio():
            data['audio'] = self.audio.to_dict(absolute_path=absolute_path)
            return data

        logger.warning(
            'Spectrogram instance does not have a path or an associated '
            'audio instance and reconstruction from dictionary values '
            'will not be possible.')
        return data


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
        kwargs = self.to_dict()

        if ref is not None:
            kwargs['ref'] = ref

        if amin is not None:
            kwargs['amin'] = amin

        if top_db is not None:
            kwargs['top_db'] = top_db

        if not self.window.is_trivial():
            kwargs['window'] = self.window

        if self.has_audio():
            kwargs['audio'] = self.audio

        if not self.is_empty() or not lazy:
            kwargs['array'] = power_to_db(self.array)

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
