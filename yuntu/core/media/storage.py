"""
Storage Module
==============

"""
import os
from abc import abstractmethod
from urllib.parse import urlparse
from contextlib import contextmanager

from yuntu.core.utils.plugins import PluginMount


class Storage(metaclass=PluginMount):
    _default = None

    @classmethod
    def get_storage(cls, path):
        default = cls.get_default()

        if default.compatible_file(path):
            return default

        return cls.infer_storage(path)

    @classmethod
    def infer_storage(cls, path):
        apt_storages = [
            storage for storage in cls.plugins if storage.compatible_file(path)
        ]

        if len(apt_storages) == 0:
            message = (
                "This file cannot be handled by the installed storage types"
            )
            raise NotImplementedError(message)

        # Return the last storage class. This way newly installed storages
        # can overwrite behaviour.
        return apt_storages[-1]

    @classmethod
    def get_default(cls):
        if cls._default is None:
            cls._default = FileSystemStorage()

        return cls._default

    @classmethod
    def set_default(cls, storage):
        if not isinstance(storage, Storage):
            raise ValueError

        cls._default = storage

    def exists(self, path):
        pass

    @abstractmethod
    @contextmanager
    def open(self, path):
        pass

    @abstractmethod
    def read(self, path):
        pass

    @abstractmethod
    def write(self, path, contents):
        pass

    @abstractmethod
    def size(self, path):
        pass

    @classmethod
    @abstractmethod
    def compatible_file(self, path):
        return False


class FileSystemStorage(Storage):
    def exists(self, path):
        return os.path.exists(path)

    @contextmanager
    def open(self, path):
        with open(path, "rb") as fsfile:
            yield fsfile

    def write(self, path, contents):
        with open(path, "wb") as fsfile:
            fsfile.write(contents)

    def read(self, path):
        with self.open(path) as f:
            return f.read()

    def size(self, path):
        return os.path.getsize(path)

    @classmethod
    def compatible_file(cls, path):
        parsed = urlparse(path)

        if parsed.scheme == "file":
            return True

        if parsed.scheme:
            # For every other scheme return False
            return False

        return True
