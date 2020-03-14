"""Media module.

This module defines the base class for all media objects in yuntu.
A media object is any object that holds information on an acoustic event.
This could be the full wav array, the zero crossing rate or the spectrogram.
These media objects can all be stored and read from the filesystem.
"""
import os
from abc import ABC
from abc import abstractmethod

import numpy as np
from yuntu.core.windows import Window


# pylint: disable=too-many-public-methods
class Media(ABC, np.ndarray):
    """Media class.

    This is the base class for all media objects in yuntu.
    """

    window_class = Window

    # pylint: disable=unused-argument
    def __new__(cls, *args, **kwargs):
        """Build Media object.

        Initialize an empty numpy array so that media object has access
        to numpy methods.
        """
        obj = np.asarray([]).view(cls)
        return obj

    # pylint: disable=no-self-use
    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Use numpy universal functions on media array."""
        modified_inputs = tuple([
            inp.array
            if isinstance(inp, Media) else inp
            for inp in inputs
        ])
        modified_kwargs = {
            key:
                value.array
                if isinstance(value, Media)
                else value
            for key, value in kwargs.items()
        }

        return getattr(ufunc, method)(*modified_inputs, **modified_kwargs)

    # pylint: disable=super-init-not-called
    def __init__(self, path=None, lazy=False, array=None, window=None, **kwargs):
        """Construct a media object."""
        self.path = path
        self.lazy = lazy

        if window is None:
            window = self.window_class()
        self.window = window

        if array is not None:
            self._array = array
        elif not lazy:
            self._array = self.load()

    def clean(self):
        """Clear media contents and free memory."""
        del self._array

    def is_empty(self):
        """Check if array has not been loaded yet."""
        return not hasattr(self, '_array') or self._array is None

    def path_exists(self):
        """Determine if the media file exists in the filesystem."""
        if self.path is None:
            return False

        return os.path.exists(self.path)

    @property
    def path_ext(self):
        """Get extension of media file."""
        _, ext = os.path.splitext(self.path)
        return ext

    @abstractmethod
    def load(self):
        """Read media object into memory."""

    @abstractmethod
    def write(self, path=None, **kwargs):
        """Write media object into filesystem."""

    @abstractmethod
    def to_dict(self):
        """Return a dictionary holding all media metadata."""

    @abstractmethod
    def plot(self, ax=None, **kwargs):  # pylint: disable=invalid-name
        """Plot a representation of the media object."""

    @property
    def array(self):
        """Get media contents."""
        if self.is_empty():
            self._array = self.load()
        return self._array

    @property
    def shape(self):
        """Get shape of media array."""
        return self.array.shape

    @property
    def flags(self):
        """Get media array flags."""
        return self.array.flags

    @property
    def strides(self):
        """Get media array strides."""
        return self.array.strides

    @property
    def ndim(self):
        """Get media array ndim."""
        return self.array.ndim

    @property
    def data(self):
        """Get media array data buffer."""
        return self.array.data

    @property
    def size(self):
        """Get media array size."""
        return self.array.size

    @property
    def itemsize(self):
        """Get media array itemsize."""
        return self.array.itemsize

    @property
    def nbytes(self):
        """Get media array nbytes."""
        return self.array.nbytes

    @property
    def base(self):
        """Get media array base."""
        return self.array.base

    @property
    def dtype(self):
        """Get media array dtype."""
        return self.array.dtype

    @property
    def flat(self):
        """Get media array flattened values."""
        return self.array.flat

    def __getitem__(self, key):
        """Get media array value."""
        return self.array[key]

    def __iter__(self):
        """Iterate over media array."""
        for value in self.array:
            yield value

    def __len__(self):
        """Get length of media array."""
        return len(self.array)

    # pylint: disable=arguments-differ
    def ravel(self, *args, **kwargs):
        """Ravel the media array."""
        return self.array.ravel(*args, **kwargs)

    # pylint: disable=arguments-differ
    def flatten(self, *args, **kwargs):
        """Flatten the media array."""
        return self.array.flatten(*args, **kwargs)

    def copy(self, *args, **kwargs):
        """Copy media element."""
        cls = type(self)
        kwargs = self.to_dict()

        if not self.is_empty():
            kwargs['array'] = self.array.copy(*args, **kwargs)

        return cls(**kwargs)

    # pylint: disable=arguments-differ
    def view(self, **kwargs):
        """Get a view of the media array."""
        return self.array.view(**kwargs)

    def __copy__(self):
        """Copy media element."""
        cls = type(self)
        kwargs = self.to_dict()

        if not self.is_empty():
            kwargs['array'] = self.array

        return cls(**kwargs)
