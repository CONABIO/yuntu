"""
Storage Module
==============

"""
from abc import abstractmethod
from contextlib import contextmanager

from yuntu.core.utils.plugins import PluginMount


class Storage(metaclass=PluginMount):
    """
    Storage interface

    Storage objects are responsible for handling access and storage of files.
    They should know when a file is available for reading, how to access and
    retrieve the contents, and in most cases how to store data in files.

    The number of types of storages available can be expanded by using the
    plugin system (see :mod:TODO)

    TODO
    """

    @abstractmethod
    def exists(self, path: str):
        pass

    @abstractmethod
    @contextmanager
    def open(self, path: str, mode: str = "rb"):
        pass

    @abstractmethod
    def read(self, path: str):
        pass

    @abstractmethod
    def write(self, path: str, contents):
        pass

    @abstractmethod
    def size(self, path: str):
        pass

    @classmethod
    @abstractmethod
    def is_compatible(self, path):
        return False

    @classmethod
    def get_default_config(cls):
        return {}
