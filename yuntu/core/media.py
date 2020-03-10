"""Media module.

This module defines the base class for all media objects in yuntu.
A media object is any object that holds information on an acoustic event.
This could be the full wav array, the zero crossing rate or the spectrogram.
These media objects can all be stored and read from the filesystem.
"""
import os
from abc import ABC
from abc import abstractmethod


class Media(ABC):
    """Media class.

    This is the base class for all media objects in yuntu.
    """

    def __init__(self, path=None, lazy=False, data=None):
        """Construct a media object."""
        self.path = path
        self.lazy = lazy

        if data is not None:
            self._data = data
        elif self.should_load():
            self._data = self.load()

    @property
    def data(self):
        """Get media contents."""
        if self.is_empty():
            self._data = self.load()
        return self._data

    def clean(self):
        """Clear media contents and free memory."""
        del self._data

    def is_empty(self):
        """Check if data has not been loaded yet."""
        return not hasattr(self, '_data') or self._data is None

    def should_load(self):
        """Determine if the media object should be read at initialization."""
        return not self.lazy

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
    def plot(self, ax=None, **kwargs):
        """Plot a representation of the media object."""
