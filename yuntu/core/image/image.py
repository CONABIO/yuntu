"""Base classes for Image manipulation."""
from typing import Optional
from typing import Dict
from typing import Any
from typing import Union
from uuid import uuid4
from collections import namedtuple
from collections import OrderedDict
import os

import numpy as np

#from yuntu.core.media.time import TimeMedia
from yuntu.core.media.atemporal import AtemporalMedia
from yuntu.core.image.utils import read_info
from yuntu.core.image.utils import read_media
from yuntu.core.image.utils import write_media
#import yuntu.core.image.image_features as features


CHANNELS = 'nchannels'
IMG_WIDTH = 'imgwidth'
IMG_HEIGHT = 'imgheight'
FILE_SIZE = 'filesize'
MEDIA_INFO_FIELDS = [
    CHANNELS,
    IMG_WIDTH,
    IMG_HEIGHT,
    FILE_SIZE
]
REQUIRED_MEDIA_INFO_FIELDS = [
    IMG_WIDTH,
    IMG_HEIGHT
#    DURATION,
#    SAMPLE_RATE
]

MediaInfo = namedtuple('MediaInfo', MEDIA_INFO_FIELDS)
MediaInfoType = Dict[str, Union[int, float]]


def media_info_is_complete(media_info: MediaInfoType) -> bool:
    """Check if media info has all required fields."""
    for field in REQUIRED_MEDIA_INFO_FIELDS:
        if field not in media_info:
            return False

    return True


class Image(AtemporalMedia):
    """Base class for all image."""

#    features_class = features.ImageFeatures

    # pylint: disable=redefined-builtin, invalid-name
    def __init__(
            self,
            path: Optional[str] = None,
            array: Optional[np.array] = None,
            imageexp: Optional[int] = 1,
            media_info: Optional[MediaInfoType] = None,
            metadata: Optional[Dict[str, Any]] = None,
            id: Optional[str] = None,
            xlen: Optional[int] = None,
            ylen: Optional[int] = None,
#            samplerate: Optional[int] = None,
#            duration: Optional[float] = None,
            resolution: Optional[float] = None,
            **kwargs):
        """Construct an Image object.

        Parameters
        ----------
        path: str, optional
            Path to audio file.
        array: np.array, optional
            Numpy array with audio data
#        timeexp: int, optional
#            Time expansion factor of audio file. Will default
#            to 1.
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
#        samplerate: int, optional
#            The samplerate used to read the audio data. If different
#            from the native sample rate, the audio will be resampled
#            at read.
        """
        if path is None and array is None:
            message = 'Either array or path must be supplied'
            raise ValueError(message)

        self.path = path
        self._imageexp = imageexp

        if id is None:
            id = os.path.basename(path)
        self.id = id

        if metadata is None:
            metadata = {}
        self.metadata = metadata
        
#        if samplerate is None:
#            samplerate = resolution

#        if ((samplerate is None) or (duration is None)) and media_info is None:
#            media_info = self.read_info()

        if media_info is not None and isinstance(media_info, dict):
            if not media_info_is_complete(media_info):
                message = (
                    f'Media info is not complete. Provided media info'
                    f'{media_info}. Required fields: {str(MEDIA_INFO_FIELDS)}')
                raise ValueError(message)
            media_info = MediaInfo(**media_info)

        self.media_info = media_info

        if xlen is None:
            xlen = self.media_info.imgwidth

        if ylen is None:
            ylen = self.media_info.imgheight


#        if samplerate is None:
#            samplerate = self.media_info.samplerate

#        if duration is None:
#            duration = self.media_info.duration

        if resolution is None:
            resolution = 1

#        self.features = self.features_class(self) #TODO

        super().__init__(
            array=array,
            path=self.path,
            resolution=resolution,
            xlen = xlen,
            ylen = ylen,
            **kwargs)

    @property
    def imageexp(self):
        return self._imageexp

    @imageexp.setter
    def imageexp(self, value):
        if self.is_empty():
            self.force_load()
        self.window.w = self.media_info.imgwidth
        self.window.h = self.media_info.imgheight


#    @property
#    def samplerate(self):
#        return self.time_axis.resolution

    @property
    def shape(self):
        return (self.media_info.nchannels,self.window.w,self.window.h)

    @classmethod
    def from_instance(
            cls,
            recording,
            lazy: Optional[bool] = False,
#            samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Image object from a database recording instance."""
        data = {
            'path': recording.path,
#            'exp': recording.timeexp,
            'media_info': recording.media_info,
            'metadata': recording.metadata,
            'lazy': lazy,
#            'samplerate': samplerate,
            'id': recording.id,
            **kwargs
        }
        return cls(**data)

    @classmethod
    def from_dict(
            cls,
            dictionary: Dict[Any, Any],
            lazy: Optional[bool] = False,
#            samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Audio object from a dictionary of metadata."""
        if 'path' not in dictionary:
            message = 'No path was provided in the dictionary argument'
            raise ValueError(message)

        dictionary['lazy'] = lazy

#        if samplerate is not None:
#            dictionary['samplerate'] = samplerate

        return cls(**dictionary, **kwargs)

    @classmethod
    def from_dataframe_row_dict(
            cls,
            dictionary: Dict[Any, Any],
            lazy: Optional[bool] = False,
#            samplerate: Optional[int] = None,
            **kwargs):
        """Create a new Audio object from a dictionary of metadata."""
        if 'path' not in dictionary:
            message = 'No path was provided in the dictionary argument'
            raise ValueError(message)

            
        media_info = {
            'nchannels': 3, # TODO
            'imgwidth': dictionary["metadata"]["media_info"]["image_width"],
            'imgheight': dictionary["metadata"]["media_info"]["image_length"],
            'filesize': dictionary["filesize"]
        }
        dictionary['media_info'] = media_info
        dictionary['lazy'] = lazy

#        if samplerate is not None:
#            dictionary['samplerate'] = samplerate

        return cls(**dictionary, **kwargs)


    @classmethod
    def from_array(
            cls,
            array: np.array,
#            samplerate: int,
            metadata: Optional[dict] = None,
            **kwargs):
        """Create a new Audio object from a numpy array."""
        shape = array.shape
        if len(shape) == 2:
            channels = 1
            w = shape[1]
            h = shape[0]
#            size = len(array)
        elif len(shape) == 3:
            channels = shape[2]
            w = shape[1]
            h = shape[0]
        else:
            message = (
                f'The array has {len(shape)} dimensions. Could not be '
                'interpreted as an image array')
            raise ValueError(message)

        media_info = {
#            SAMPLE_RATE: samplerate,
#            SAMPLE_WIDTH: 16,
#            CHANNELS: channels,
#            LENGTH: size,
#            FILE_SIZE: size * 16 * channels,
#            DURATION: size / samplerate
            CHANNELS: channels,
            IMG_WIDTH: w,
            IMG_HEIGHT: h,
            FILE_SIZE: w * h * channels * 16 # 16?
        }

        return cls(
            array=array,
            media_info=media_info,
            id=str(uuid4()),
            metadata=metadata,
            **kwargs)

    def _copy_dict(self, **kwargs):
        return {
#            'timeexp': self.timeexp,
            'media_info': self.media_info,
            'metadata': self.metadata,
            'id': self.id,
            **super()._copy_dict(**kwargs),
        }

    def read_info(self, path=None):
        if path is None:
            if self.is_remote():
                path = self.remote_load()
                self._buffer = path
                return read_info(path)

            path = self.path
        return read_info(path)

    def load_from_path(self, path=None):
        """Read signal from file."""
        if path is None:
            path = self.path

        if hasattr(self, '_buffer'):
            path = self._buffer

#        start = self._get_start()
#        end = self._get_end()
#        duration = end - start
        x = self._get_x()
        y = self._get_y()
        w = self._get_w()
        h = self._get_h()

        signal = read_media(
            path, x,y,w,h)
#            self.samplerate,
#            offset=start,
#            duration=duration)

        if hasattr(self, '_buffer'):
            self._buffer.close()
            del self._buffer

        return signal

    # pylint: disable=arguments-differ
    def write(
            self,
            path: str,
            media_format: Optional[str] = "jpg"):
        """Write media to path."""
        self.path = path

        image = self.array
#        if samplerate is None:
#            samplerate = self.samplerate

        write_media(self.path,
                    image)
#                    samplerate,
#                    self.media_info.nchannels,
#                    media_format)

    def view(self):
        """Return HTML5 audio element player of current audio."""
        # pylint: disable=import-outside-toplevel
        from IPython.display import Image as HTMLImage
#        rate = self.samplerate * speed
        return HTMLImage(data=self.array)

    def plot(self, ax=None, **kwargs):
        """Plot soundwave in the given axis."""
        ax = super().plot(ax=ax, **kwargs)

        image = self.array.copy()
        minimum = image.min()
        maximum = image.max()

        
        vmin = kwargs.get('vmin', None)
        vmax = kwargs.get('vmax', None)

        if 'pvmin' in kwargs:
            pvmin = kwargs['pvmin']
            vmin = minimum + (maximum - minimum) * pvmin

        if 'pvmax' in kwargs:
            pvmax = kwargs['pvmax']
            vmax = minimum + (maximum - minimum) * pvmax

        ax.plot(
            self.x_size,
            self.y_size,
            image,
#            c=kwargs.get('color', None),
#            linewidth=kwargs.get('linewidth', 1),
#            linestyle=kwargs.get('linestyle', None),
            vmin=vmin,
            vmax=vmax,
            alpha=kwargs.get('alpha', 1))

        return ax

    def to_dict(self):
        """Return a dictionary holding all image metadata."""
        if self.media_info is None:
            media_info = None
        else:
            media_info = dict(self.media_info._asdict())

        return {
#            'timeexp': self.timeexp,
            'media_info': media_info,
            'metadata': self.metadata.copy(),
            'id': self.id,
            **super().to_dict()
        }

    def __repr__(self):
        """Return a representation of the image object."""
        data = OrderedDict()
        if self.path is not None:
            data['path'] = repr(self.path)
        else:
            data['array'] = repr(self.array)

        data['x'] = repr(self._get_x())
        data['y'] = repr(self._get_y())
        data['w'] = repr(self.media_info.imgwidth) #._get_w()
        data['h'] = repr(self.media_info.imgheight)
#        data['duration'] = self.duration
#        data['samplerate'] = self.samplerate

        if self.imageexp != 1:
            data['imageexp'] = self.imageexp

        if not self._has_trivial_window():
            data['window'] = repr(self.window)

        args = [f'{key}={value}' for key, value in data.items()]
        args_string = ', '.join(args)
        return f'Image({args_string})'
