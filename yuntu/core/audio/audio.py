"""Base classes for audio manipulation."""
from typing import Optional
from typing import Dict
from typing import Any
from typing import Union
from uuid import uuid4
from collections import namedtuple
from collections import OrderedDict
import os

import numpy as np


from yuntu.core.windows import TimeWindow
from yuntu.logging import logger
from yuntu.core.media.time import TimeMedia
from yuntu.core.media.time import TimeMediaMixin
import yuntu.core.media.masked as masked_media
from yuntu.core.audio.utils import read_info
from yuntu.core.audio.utils import read_media
from yuntu.core.audio.utils import write_media
import yuntu.core.audio.audio_features as features


CHANNELS = 'nchannels'
SAMPLE_WIDTH = 'sampwidth'
SAMPLE_RATE = 'samplerate'
LENGTH = 'length'
FILE_SIZE = 'filesize'
DURATION = 'duration'
MEDIA_INFO_FIELDS = [
    CHANNELS,
    SAMPLE_WIDTH,
    SAMPLE_RATE,
    LENGTH,
    FILE_SIZE,
    DURATION,
]

MediaInfo = namedtuple('MediaInfo', MEDIA_INFO_FIELDS)
MediaInfoType = Dict[str, Union[int, float]]


def media_info_is_complete(media_info: MediaInfoType) -> bool:
    """Check if media info has all required fields."""
    for field in MEDIA_INFO_FIELDS:
        if field not in media_info:
            return False

    return True


class Audio(TimeMedia):
    """Base class for all audio."""

    features_class = features.AudioFeatures

    # pylint: disable=redefined-builtin, invalid-name
    def __init__(
            self,
            path: Optional[str] = None,
            array: Optional[np.array] = None,
            timeexp: Optional[int] = 1,
            media_info: Optional[MediaInfoType] = None,
            metadata: Optional[Dict[str, Any]] = None,
            id: Optional[str] = None,
            samplerate: Optional[int] = None,
            duration: Optional[float] = None,
            resolution: Optional[float] = None,
            **kwargs):
        """Construct an Audio object.

        Parameters
        ----------
        path: str, optional
            Path to audio file.
        array: np.array, optional
            Numpy array with audio data
        timeexp: int, optional
            Time expansion factor of audio file. Will default
            to 1.
        media_info: dict, optional
            Dictionary holding all audio file media information.
            This information consists of number of channels (nchannels),
            sample width in bytes (sampwidth), sample rate in Hertz
            (samplerate), length of wav array (length), duration of
            audio in seconds (duration), and file size in bytes (filesize).
        metadata: dict, optional
            A dictionary holding any additional information on the
            audio file.
        id: str, optional
            A identifier for this audiofile.
        lazy: bool, optional
            A boolean flag indicating whether loading of audio data
            is done only when required. Defaults to false.
        samplerate: int, optional
            The samplerate used to read the audio data. If different
            from the native sample rate, the audio will be resampled
            at read.
        """
        if path is None and array is None:
            message = 'Either array or path must be supplied'
            raise ValueError(message)

        self.path = path
        self.timeexp = timeexp

        if id is None:
            id = os.path.basename(path)
        self.id = id

        if metadata is None:
            metadata = {}
        self.metadata = metadata

        if media_info is None:
            media_info = self.read_info()

        if not isinstance(media_info, MediaInfo):
            if not media_info_is_complete(media_info):
                message = (
                    f'Media info is not complete. Provided media info'
                    f'{media_info}. Required fields: {str(MEDIA_INFO_FIELDS)}')
                raise ValueError(message)
            media_info = MediaInfo(**media_info)

        self.media_info = media_info

        if samplerate is None:
            samplerate = self.media_info.samplerate

        if resolution is None:
            resolution = samplerate

        if duration is None:
            duration = self.media_info.duration

        self.features = self.features_class(self)

        super().__init__(
            array=array,
            path=self.path,
            duration=duration,
            resolution=resolution,
            **kwargs)

    @property
    def samplerate(self):
        return self.resolution

    @classmethod
    def from_instance(
            cls,
            recording,
            lazy: Optional[bool] = False,
            samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Audio object from a database recording instance."""
        data = {
            'path': recording.path,
            'timeexp': recording.timeexp,
            'media_info': recording.media_info,
            'metadata': recording.metadata,
            'lazy': lazy,
            'samplerate': samplerate,
            'id': recording.id,
            **kwargs
        }
        return cls(**data)

    @classmethod
    def from_dict(
            cls,
            dictionary: Dict[Any, Any],
            lazy: Optional[bool] = False,
            samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Audio object from a dictionary of metadata."""
        if 'path' not in dictionary:
            message = 'No path was provided in the dictionary argument'
            raise ValueError(message)

        window = dictionary.pop('window', None)
        if window is not None:
            dictionary['window'] = TimeWindow.from_dict(window)

        if lazy:
            dictionary['lazy'] = True

        if samplerate is not None:
            dictionary['samplerate'] = samplerate

        return cls(**dictionary, **kwargs)

    @classmethod
    def from_array(
            cls,
            array: np.array,
            samplerate: int,
            metadata: Optional[dict] = None,
            **kwargs):
        """Create a new Audio object from a numpy array."""
        shape = array.shape
        if len(shape) == 1:
            channels = 1
            size = len(array)
        elif len(shape) == 2:
            channels = shape[0]
            size = shape[1]
        else:
            message = (
                f'The array has {len(shape)} dimensions. Could not be '
                'interpreted as an audio array')
            raise ValueError(message)

        media_info = {
            SAMPLE_RATE: samplerate,
            SAMPLE_WIDTH: 16,
            CHANNELS: channels,
            LENGTH: size,
            FILE_SIZE: size * 16 * channels,
            DURATION: size / samplerate
        }

        return cls(
            array=array,
            media_info=media_info,
            id=str(uuid4()),
            metadata=metadata,
            **kwargs)

    def _copy_dict(self, **kwargs):
        return {
            'timeexp': self.timeexp,
            'media_info': self.media_info,
            'metadata': self.metadata,
            'id': self.id,
            **super()._copy_dict(**kwargs),
        }

    def read_info(self):
        if self.is_remote():
            self.path = self.remote_load()

        return read_info(self.path, self.timeexp)

    def load(self):
        """Read signal from file."""
        if self.is_remote():
            self.path = self.remote_load()

        start = self._get_start()
        end = self._get_end()
        duration = end - start

        signal, _ = read_media(
            self.path,
            self.samplerate,
            offset=start,
            duration=duration)
        return signal

    # pylint: disable=arguments-differ
    def write(
            self,
            path: str,
            media_format: Optional[str] = "wav",
            samplerate: Optional[int] = None):
        """Write media to path."""
        self.path = path

        signal = self.array
        if samplerate is None:
            samplerate = self.samplerate

        write_media(self.path,
                    signal,
                    samplerate,
                    self.media_info.nchannels,
                    media_format)

    def listen(self, speed: Optional[float] = 1):
        """Return HTML5 audio element player of current audio."""
        # pylint: disable=import-outside-toplevel
        from IPython.display import Audio as HTMLAudio
        rate = self.media_info.samplerate * speed
        return HTMLAudio(data=self.array, rate=rate)

    def plot(self, ax=None, **kwargs):
        """Plot soundwave in the given axis."""
        # pylint: disable=import-outside-toplevel
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', None))

        array = self.array.copy()
        if 'vmax' in kwargs:
            maximum = np.abs(array).max()
            vmax = kwargs['vmax']
            array *= vmax / maximum

        if 'offset' in kwargs:
            array += kwargs['offset']

        lineplot, = ax.plot(
            self.times,
            array,
            c=kwargs.get('color', None),
            linewidth=kwargs.get('linewidth', 1),
            linestyle=kwargs.get('linestyle', None),
            alpha=kwargs.get('alpha', 1))
        color = lineplot.get_color()

        xlabel = kwargs.get('xlabel', False)
        if xlabel:
            if not isinstance(xlabel, str):
                xlabel = 'Time (s)'
            ax.set_xlabel(xlabel)

        ylabel = kwargs.get('ylabel', False)
        if ylabel:
            if not isinstance(ylabel, str):
                ylabel = 'Amplitude'
            ax.set_ylabel(ylabel)

        title = kwargs.get('title', False)
        if title:
            if not isinstance(title, str):
                title = 'Waveform'
            ax.set_title(title)

        if kwargs.get('window', False):
            linestyle = kwargs.get('window_linestyle', '--')
            ax.axvline(self._get_start(), color=color, linestyle=linestyle)
            ax.axvline(self._get_end(), color=color, linestyle=linestyle)

        return ax

    def to_dict(self, absolute_path=True):
        """Return a dictionary holding all audio metadata."""
        data = {
            'timeexp': self.timeexp,
            'media_info': dict(self.media_info._asdict()),
            'metadata': self.metadata.copy(),
            'id': self.id,
            'window': self.window.to_dict(),
            'samplerate': self.samplerate
        }

        if self.path_exists():
            if absolute_path:
                data['path'] = os.path.abspath(self.path)
            else:
                data['path'] = self.path
        else:
            message = (
                'Audio instance does not have a path and its reconstruction '
                'will not be possible from the dictionary information.')
            logger.warning(message)

        return data

    def __repr__(self):
        """Return a representation of the audio object."""
        data = OrderedDict()
        if self.path is not None:
            data['path'] = repr(self.path)
        else:
            data['array'] = repr(self.array)
            data['media_info'] = repr(self.media_info)

        if self.timeexp != 1:
            data['timeexp'] = 1

        if self.metadata:
            data['metadata'] = repr(self.metadata)

        if self.window.start or self.window.end:
            data['window'] = repr(self.window)

        args = [f'{key}={value}' for key, value in data.items()]
        args_string = ', '.join(args)
        return f'Audio({args_string})'


@masked_media.masks(Audio)
class MaskedAudio(TimeMediaMixin, masked_media.MaskedMedia):
    def plot(self, ax=None, **kwargs):
        # pylint: disable=import-outside-toplevel
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', None))

        ax.pcolormesh(
            self.times,
            [0, 1],
            np.array([self.array]),
            cmap=kwargs.get('cmap', 'gray'),
            alpha=kwargs.get('alpha', 1))

        xlabel = kwargs.get('xlabel', False)
        if xlabel:
            if not isinstance(xlabel, str):
                xlabel = 'Time (s)'
            ax.set_xlabel(xlabel)

        title = kwargs.get('title', False)
        if title:
            if not isinstance(title, str):
                title = 'Mask'
            ax.set_title(title)

        if kwargs.get('window', False):
            linestyle = kwargs.get('window_linestyle', '--')
            color = kwargs.get('window_color', 'red')
            ax.axvline(self._get_start(), color=color, linestyle=linestyle)
            ax.axvline(self._get_end(), color=color, linestyle=linestyle)

        return ax
