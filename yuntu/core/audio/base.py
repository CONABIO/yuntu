"""Base classes for audio manipulation."""
from typing import Optional
from typing import Dict
from typing import Any
from typing import Union
from uuid import uuid4
import os
from abc import ABC, abstractmethod
import numpy as np

from yuntu.core.audio.utils import read_info
from yuntu.core.audio.utils import read_media
from yuntu.core.audio.utils import write_media


class Media(ABC):
    """Abstract class for any media object."""

    @abstractmethod
    def read(self):
        """Read media from file."""

    @abstractmethod
    def write(self, path, out_format):
        """Write media to path."""


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

MediaInfoType = Dict[str, Union[int, float]]


def media_info_is_complete(media_info: MediaInfoType) -> bool:
    """Check if media info has all required fields."""
    for field in MEDIA_INFO_FIELDS:
        if field not in media_info:
            return False

    return True


class Audio(Media):
    """Base class for all audio."""

    def __init__(
            self,
            path: Optional[str] = None,
            data: Optional[np.array] = None,
            timeexp: Optional[int] = 1,
            media_info: Optional[MediaInfoType] = None,
            metadata: Optional[Dict[str, Any]] = None,
            id: Optional[str] = None,
            lazy: Optional[bool] = False,
            read_samplerate: Optional[int] = None):
        """Construct an Audio object.

        Parameters
        ----------
        path: str, optional
            Path to audio file.
        data: np.array, optional
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
        if path is None and data is None:
            message = 'Either data or path must be supplied'
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
            media_info = read_info(self.path, self.timeexp)
        if not media_info_is_complete(media_info):
            message = (
                f'Media info is not complete. Provided media info'
                f'{media_info}. Required fields: {str(MEDIA_INFO_FIELDS)}')
            raise ValueError(message)
        self.media_info = media_info

        if read_samplerate is None:
            read_samplerate = self.media_info[SAMPLE_RATE]
        self.read_samplerate = read_samplerate

        self._data = data
        if not lazy and data is None:
            self._data = self._load()

    @classmethod
    def from_instance(
            cls,
            recording,
            lazy: Optional[bool] = False,
            read_samplerate: Optional[int] = None):
        """Create a new Audio object from a database recording instance."""
        data = {
            'db_entry': recording,
            'timeexp': recording.timeexp,
            'media_info': recording.media_info,
            'metadata': recording.metadata,
            'lazy': lazy,
            'read_samplerate': read_samplerate,
            'id': recording.id,
        }
        return cls(recording.path, **data)

    @classmethod
    def from_dict(
            cls,
            dictionary: Dict[Any, Any],
            lazy: Optional[bool] = False,
            read_samplerate: Optional[int] = None):
        """Create a new Audio object from a dictionary of metadata."""
        if 'path' not in dictionary:
            message = 'No path was provided in the dictionary argument'
            raise ValueError(message)

        path = dictionary.pop('path')

        if lazy:
            dictionary['lazy'] = True

        if read_samplerate is not None:
            dictionary['read_samplerate'] = read_samplerate

        return cls(path, **dictionary)

    @classmethod
    def from_array(
            cls,
            array: np.array,
            samplerate: int):
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

        return Audio(data=array, media_info=media_info, id=str(uuid4()))

    @property
    def data(self):
        """Return the wav data."""
        if not hasattr(self, '_data') or self._data is None:
            self._data = self._load()
        return self._data

    @property
    def times(self):
        """Get the time array.

        This is an array of the same length as the wav data array and holds
        the time (in seconds) corresponding to each piece of the wav array.
        """
        length = self.media_info[LENGTH]
        duration = self.media_info[DURATION]
        return np.linspace(0, duration, length)

    def resample(
            self,
            samplerate: int,
            lazy: Optional[bool] = False):
        """Get a new Audio object with the resampled audio."""
        return Audio(
            self.path,
            timeexp=self.timeexp,
            media_info=self.media_info.copy(),
            metadata=self.metadata.copy(),
            id=self.id,
            lazy=lazy,
            read_samplerate=samplerate,
            db_entry=self.db_entry)

    def slice(self, limits=None):
        """Return a new Audio object with mask initialized at limits."""
        if limits is not None:
            offset = limits[0]
            duration = limits[1] - limits[0]
            mask = (offset, duration)
        else:
            mask = None
        return Audio(self.meta, mask)

    def set_mask(self, limits=None):
        """Set read mask.

        A read mask is a time interval that determines the part of
        the recording that is going to be read and affects any output that
        uses loaded data.
        """
        if limits is not None:
            offset = limits[0]
            duration = limits[1] - limits[0]
            self.mask = (offset, duration)
        else:
            self.mask = None
        self.clear()

    def unset_mask(self):
        """Unset read mask."""
        self.set_mask()

    def clear(self):
        """Clear cached data."""
        del self._data

    def get_index_from_time(self, time):
        """Get the index of the wav array corresponding to the given time."""
        if time < 0:
            raise ValueError('No negative times are allowed')

        if time > self.media_info[DURATION]:
            raise ValueError('Requested time is larger than audio duration')

        index = int(time * self.media_info[SAMPLE_RATE])
        return index

    def read(self, start=None, end=None):
        """Read a section of the wav array.

        Parameters
        ----------
        start: float, optional
            Time at which read starts, in seconds. If not provided
            start will be defined as the recording start. Should
            be larger than 0.
        end: float, optional
            Time at which read ends, in seconds. If not provided
            end will be defined as the recording end. Should be
            less than the duration of the audio.

        Returns
        -------
        np.array
            The wav data contained in the demanded temporal limits.

        Raises
        ------
        ValueError
            When start is less than end, or end is larger than the
            duration of the audio, or start is less than 0.
        """
        if start is None:
            start = 0

        if end is None:
            end = self.media_info[LENGTH]

        if start > end:
            message = 'Read start should be less than read end.'
            raise ValueError(message)

        start_index = self.get_index_from_time(start)
        end_index = self.get_index_from_time(end)
        return self.data[start_index: end_index + 1]

    def _load(self):
        """Read signal from file (mask sensitive, lazy loading)."""
        signal, _ = read_media(self.path, self.read_samplerate)
        return signal

    def write(
            self,
            path: str,
            media_format: Optional[str] = "wav",
            samplerate: Optional[int] = None):
        """Write media to path."""
        signal = self.data

        out_sr = self.media_info[SAMPLE_RATE]
        if samplerate is not None:
            out_sr = samplerate

        write_media(self.path,
                    signal,
                    out_sr,
                    self.media_info[CHANNELS],
                    media_format)

    def listen(self, speed_modifier: Optional[float] = 1):
        """Return HTML5 audio element player of current audio."""
        # pylint: disable=import-outside-toplevel
        from IPython.display import Audio as HTMLAudio
        rate = self.media_info[SAMPLE_RATE] * speed_modifier
        return HTMLAudio(data=self.data, rate=rate)

    def plot(self, ax=None, **kwargs):
        """Plot soundwave in the given axis."""
        # pylint: disable=import-outside-toplevel
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.pop('figsize', None))

        ax.plot(self.times, self.data, **kwargs)
        return ax
