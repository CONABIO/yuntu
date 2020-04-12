"""Spectrogram class module."""
from typing import Optional
import os
from collections import namedtuple
from collections import OrderedDict

import numpy as np
from librosa.core import amplitude_to_db
from librosa.core import power_to_db

from yuntu.logging import logger
from yuntu.core.windows import Window
import yuntu.core.audio.audio as audio
from yuntu.core.audio.features.base import TimeFrequencyFeature
from yuntu.core.media.time_frequency import TimeFrequencyMediaMixin
from yuntu.core.media.time_frequency import TimeFreqResolution
from yuntu.core.audio.features.spectral import stft
import yuntu.core.media.masked as masked_media


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


class Spectrogram(TimeFrequencyFeature):
    """Spectrogram class."""

    units = 'amplitude'

    def __init__(
            self,
            n_fft=N_FFT,
            hop_length=HOP_LENGTH,
            window_function=WINDOW_FUNCTION,
            audio=None,
            max_freq=None,
            resolution=None,
            array=None,
            duration=None,
            **kwargs):
        """Construct Spectrogram object."""
        self.n_fft = n_fft
        self.hop_length = hop_length
        self.window_function = window_function

        if duration is None:
            if audio is None:
                message = (
                    'If no audio is provided a duration must be set')
                raise ValueError(message)
            duration = audio.duration

        if resolution is None:
            if array is not None:
                columns = array.shape[self.frequency_axis]
                time_resolution = columns / duration
            elif audio is not None:
                time_resolution = audio.samplerate / hop_length
            else:
                message = (
                    'If no audio or array is provided a samplerate must be '
                    'set')
                raise ValueError(message)

            rows = 1 + n_fft // 2
            if max_freq is None:
                max_freq = time_resolution * hop_length // 2

            freq_resolution = rows / max_freq
            resolution = TimeFreqResolution(
                time=time_resolution,
                freq=freq_resolution)

        if not isinstance(resolution, TimeFreqResolution):
            resolution = TimeFreqResolution(*resolution)

        if max_freq is None:
            max_freq = resolution.time * hop_length // 2

        super().__init__(
            audio=audio,
            max_freq=max_freq,
            array=array,
            resolution=resolution,
            duration=duration,
            **kwargs)

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

        if not self._has_trivial_window():
            data['window'] = repr(self.window)

        class_name = type(self).__name__
        args = [f'{key}={value}' for key, value in data.items()]
        args_string = ', '.join(args)

        return f'{class_name}({args_string})'

    def calculate(self):
        """Calculate spectrogram from audio data.

        Uses the spectrogram instance configurations for stft
        calculation.

        Returns
        -------
        np.array
            Calculated spectrogram.
        """
        if not self._has_trivial_window():
            start = self._get_start()
            end = self._get_end()
            array = self.audio.cut(
                start_time=start, end_time=end).array
        else:
            array = self.audio.array

        result = np.abs(stft(
            array,
            n_fft=self.n_fft,
            hop_length=self.hop_length,
            window=self.window_function))

        if self._has_trivial_window():
            return result

        max_freq = self._get_max()
        min_freq = self._get_min()
        rows = 1 + self.n_fft // 2
        max_index = int(rows * (max_freq / self.max_freq))
        min_index = int(rows * (min_freq / self.max_freq))
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
            if self.audio.path_exists():
                data['audio_path'] = self.audio.path

        if not self._has_trivial_window():
            window_data = {
                f'window_{key}': value
                for key, value in self.window.to_dict()
                if value is not None
            }
            data.update(window_data)

        np.savez(self.path, **data)

    @property
    def shape(self) -> Shape:
        """Get spectrogram shape."""
        return Shape(rows=self.frequency_size, columns=self.time_size)

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
            _, ax = plt.subplots(figsize=kwargs.get('figsize', None))

        spectrogram = self.array
        minimum = spectrogram.min()
        maximum = spectrogram.max()

        vmin = kwargs.get('vmin', None)
        vmax = kwargs.get('vmax', None)

        if 'pvmin' in kwargs:
            pvmin = kwargs['pvmin']
            vmin = minimum + (maximum - minimum) * pvmin

        if 'pvmax' in kwargs:
            pvmax = kwargs['pvmax']
            vmax = minimum + (maximum - minimum) * pvmax

        mesh = ax.pcolormesh(
            self.times,
            self.frequencies,
            spectrogram,
            vmin=vmin,
            vmax=vmax,
            cmap=kwargs.get('cmap', 'gray'),
            alpha=kwargs.get('alpha', 1.0))

        if kwargs.get('colorbar', False):
            plt.colorbar(mesh, ax=ax)

        xlabel = kwargs.get('xlabel', False)
        if xlabel:
            if not isinstance(xlabel, str):
                xlabel = 'Time (s)'
            ax.set_xlabel(xlabel)

        ylabel = kwargs.get('ylabel', False)
        if ylabel:
            if not isinstance(ylabel, str):
                ylabel = 'Frequency (Hz)'
            ax.set_ylabel(ylabel)

        title = kwargs.get('title', False)
        if title:
            if not isinstance(title, str):
                title = f'Spectrogram ({self.units})'
            ax.set_title(title)

        if kwargs.get('window', False):
            min_freq = self._get_min()
            max_freq = self._get_max()
            start_time = self._get_start()
            end_time = self._get_end()
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
                color=kwargs.get('window_color', None),
                linewidth=kwargs.get('window_linewidth', 3),
                linestyle=kwargs.get('window_linestyle', '--'))

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
        kwargs = self._copy_dict()

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
        kwargs = {
            'ref': ref,
            'amin': amin,
            'top_db': top_db,
            **self._copy_dict()
        }

        if not self.is_empty() or not lazy:
            kwargs['array'] = amplitude_to_db(self.array)

        return DecibelSpectrogram(**kwargs)

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

    @classmethod
    def from_dict(cls, data):
        units = data.pop('units', None)

        if 'audio' in data:
            data['audio'] = audio.Audio.from_dict(data['audio'])

        if 'window' in data:
            data['window'] = Window.from_dict(data['window'])

        if units == 'amplitude':
            return Spectrogram(**data)

        if units == 'power':
            return PowerSpectrogram(**data)

        if units == 'db':
            return DecibelSpectrogram(**data)

        raise ValueError('Unknown or missing units')


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
        kwargs['annotations'] = self.annotations.annotations
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


@masked_media.masks(Spectrogram)
class MaskedSpectrogram(TimeFrequencyMediaMixin, masked_media.MaskedMedia):
    def plot(self, ax=None, **kwargs):
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', None))

        ax.pcolormesh(
            self.times,
            self.frequencies,
            self.array,
            cmap=kwargs.get('cmap', 'gray'),
            alpha=kwargs.get('alpha', 1.0))

        xlabel = kwargs.get('xlabel', False)
        if xlabel:
            if not isinstance(xlabel, str):
                xlabel = 'Time (s)'
            ax.set_xlabel(xlabel)

        ylabel = kwargs.get('ylabel', False)
        if ylabel:
            if not isinstance(ylabel, str):
                ylabel = 'Frequency (Hz)'
            ax.set_ylabel(ylabel)

        title = kwargs.get('title', False)
        if title:
            if not isinstance(title, str):
                title = f'Spectrogram Mask'
            ax.set_title(title)

        if kwargs.get('window', False):
            min_freq = self._get_min()
            max_freq = self._get_max()
            start_time = self._get_start()
            end_time = self._get_end()
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
                color=kwargs.get('window_color', None),
                linewidth=kwargs.get('window_linewidth', 3),
                linestyle=kwargs.get('window_linestyle', '--'))

        return ax
