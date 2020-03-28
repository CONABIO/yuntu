"""Base classes for audio manipulation."""
from typing import Optional
from typing import Dict
from typing import Any
from typing import Union
from uuid import uuid4
from urllib.parse import urlparse
from collections import namedtuple
from collections import OrderedDict
import os

import numpy as np

from yuntu.utils import download_file
from yuntu.utils import scp_file
from yuntu.utils import tmp_file
from yuntu.core.windows import TimeWindow
from yuntu.logging import logger
from yuntu.core.media import Media
from yuntu.core.annotation.annotated_object import AnnotatedObject
from yuntu.core.audio.utils import read_info
from yuntu.core.audio.utils import read_media
from yuntu.core.audio.utils import write_media
from yuntu.core.audio.utils import resample
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


class Audio(Media):
    """Base class for all audio."""

    window_class = TimeWindow
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
            read_samplerate: Optional[int] = None,
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
        read_samplerate: int, optional
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

        if not media_info_is_complete(media_info):
            message = (
                f'Media info is not complete. Provided media info'
                f'{media_info}. Required fields: {str(MEDIA_INFO_FIELDS)}')
            raise ValueError(message)

        self.media_info = MediaInfo(**media_info)

        if read_samplerate is None:
            read_samplerate = self.media_info.samplerate
        self.read_samplerate = read_samplerate

        self.features = self.features_class(self)

        super().__init__(array=array, path=self.path, **kwargs)

    @classmethod
    def from_instance(
            cls,
            recording,
            lazy: Optional[bool] = False,
            read_samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Audio object from a database recording instance."""
        data = {
            'path': recording.path,
            'timeexp': recording.timeexp,
            'media_info': recording.media_info,
            'metadata': recording.metadata,
            'lazy': lazy,
            'read_samplerate': read_samplerate,
            'id': recording.id,
            **kwargs
        }
        return cls(**data)

    @classmethod
    def from_dict(
            cls,
            dictionary: Dict[Any, Any],
            lazy: Optional[bool] = False,
            read_samplerate: Optional[int] = None,
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

        if read_samplerate is not None:
            dictionary['read_samplerate'] = read_samplerate

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

    @property
    def times(self):
        """Get the time array.

        This is an array of the same length as the wav data array and holds
        the time (in seconds) corresponding to each piece of the wav array.
        """
        start = self._get_start()
        end = self._get_end()

        if self.is_empty():
            duration = end - start
            length = int(duration * self.media_info.samplerate)
        else:
            length = len(self.array)

        return np.linspace(start, end, length)

    def resample(
            self,
            samplerate: int,
            lazy: Optional[bool] = False,
            **kwargs):
        """Get a new Audio object with the resampled audio."""
        audio_info = self.to_dict()
        audio_info['window'] = self.window
        audio_info['read_samplerate'] = samplerate
        audio_info['media_info']['samplerate'] = samplerate
        audio_info['lazy'] = lazy

        if not self.path_exists():
            data = resample(
                self.array,
                self.read_samplerate,
                samplerate,
                **kwargs)
            audio_info['array'] = data

        return Audio(**audio_info)

    def get_index_from_time(self, time):
        """Get the index of the wav array corresponding to the given time."""
        start = self._get_start()
        if time < start:
            message = (
                'Time earlier that start of recording file or window start '
                'was requested')
            raise ValueError(message)

        if time > self._get_end():
            message = (
                'Time earlier that start of recording file or window start '
                'was requested')
            raise ValueError('Requested time is larger than audio duration')

        index = int((time - start) * self.media_info.samplerate)
        return index

    def _get_start(self):
        if self.window.start is not None:
            return self.window.start
        return 0

    def _get_end(self):
        if self.window.end is not None:
            return self.window.end
        return self.media_info.duration

    def read(self, start=None, end=None):
        """Read a section of the wav array.

        Parameters
        ----------
        start: float, optional
            Time at which read starts, in seconds. If not provided
            start will be defined as the recording start. Should
            be larger than 0. If a non trivial window is set, the
            provided starting time should be larger that the window
            starting time.
        end: float, optional
            Time at which read ends, in seconds. If not provided
            end will be defined as the recording end. Should be
            less than the duration of the audio. If a non trivial
            window is set, the provided ending time should be
            larger that the window ending time.

        Returns
        -------
        np.array
            The wav data contained in the demanded temporal limits.

        Raises
        ------
        ValueError
            When start is less than end, or end is larger than the
            duration of the audio, or start is less than 0. If a non
            trivial window is set, it will also throw an error if
            the requested starting and ending times are smaller or
            larger that those set by the window.
        """
        if start is None:
            start = self._get_start()

        if end is None:
            end = self._get_end()

        if start > end:
            message = 'Read start should be less than read end.'
            raise ValueError(message)

        start_index = self.get_index_from_time(start)
        end_index = self.get_index_from_time(end)
        return self.array[start_index: end_index + 1]

    def is_remote(self):
        if os.path.exists(self.path):
            return False

        parsed = urlparse(self.path)

        if not parsed.scheme:
            return False

        if parsed.scheme == 'file':
            return False

        return True

    def remote_load(self):
        parsed = urlparse(self.path)

        if parsed.scheme in ['http', 'https']:
            filename = os.path.basename(parsed.path)
            with tmp_file(filename) as (name, tmpfile):
                download_file(self.path, tmpfile)

            return name

        if parsed.scheme == 'scp':
            filename = os.path.basename(parsed.path)
            path = parsed.netloc+":"+parsed.path.replace("//", "/")
            return scp_file(src=path, dest=filename)

        message = (
            'Remote loading is not implemented for '
            f'scheme {parsed.scheme}')
        raise NotImplementedError(message)

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
            self.read_samplerate,
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
        out_sr = self.media_info.samplerate
        if samplerate is not None:
            out_sr = samplerate

        write_media(self.path,
                    signal,
                    out_sr,
                    self.media_info.nchannels,
                    media_format)

    def listen(self, speed_modifier: Optional[float] = 1):
        """Return HTML5 audio element player of current audio."""
        # pylint: disable=import-outside-toplevel
        from IPython.display import Audio as HTMLAudio
        rate = self.media_info.samplerate * speed_modifier
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

    def to_mask(self, geometry):
        """Return masked 1d array."""
        start, _, end, _ = geometry.bounds
        start_index = self.get_index_from_time(start)
        end_index = self.get_index_from_time(end)

        mask = np.zeros(self.media_info.length)
        if end_index > start_index:
            mask[start_index: end_index] = 1
        else:
            mask[start_index] = 1
        return mask

    def to_dict(self, absolute_path=True):
        """Return a dictionary holding all audio metadata."""
        data = {
            'timeexp': self.timeexp,
            'media_info': dict(self.media_info._asdict()),
            'metadata': self.metadata.copy(),
            'id': self.id,
            'window': self.window.to_dict(),
            'read_samplerate': self.read_samplerate
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

    def cut(
            self,
            start_time: float = None,
            end_time: float = None,
            window: TimeWindow = None,
            lazy=True):
        """Get a window to the audio data.

        Parameters
        ----------
        start: float, optional
            Window starting time in seconds. If not provided
            it will default to the beggining of the recording.
        end: float, optional
            Window ending time in seconds. If not provided
            it will default to the duration of the recording.
        window: TimeWindow, optional
            A window object to use for cutting.
        lazy: bool, optional
            Boolean flag that determines if the fragment loads
            its data lazily.

        Returns
        -------
        Audio
            The resulting audio object with the correct window set.
        """
        current_start = self._get_start()
        current_end = self._get_end()

        if start_time is None:
            start_time = window.start if window.start is not None else current_start

        if end_time is None:
            end_time = window.end if window.end is not None else current_end

        start_time = max(min(start_time, current_end), current_start)
        end_time = max(min(end_time, current_end), current_start)

        if end_time < start_time:
            message = 'Window is empty'
            raise ValueError(message)

        kwargs_dict = self.to_dict()
        kwargs_dict['window'] = TimeWindow(start=start_time, end=end_time)
        kwargs_dict['lazy'] = lazy

        if not self.is_empty():
            if start_time is not None:
                start_time = self.get_index_from_time(start_time)

            if end_time is not None:
                end_time = self.get_index_from_time(end_time)

            data = self.array[slice(start_time, end_time)]
            kwargs_dict['data'] = data.copy()

        return Audio(**kwargs_dict)
