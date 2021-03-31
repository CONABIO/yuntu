from abc import ABC
from abc import abstractmethod


class Media(ABC):
    """
    Media objects.

    A media object represents a either the contents of a file or
    a object that could be stored in a file.
    """

    def __init__(self, path=None, buffer=None, lazy=None):
        if path is None and buffer is None:
            message = "A path or a buffer must be provided"
            raise ValueError(message)

        self.path = path
        self.buffer = buffer
        self.lazy = lazy

    def load(self):
        pass
