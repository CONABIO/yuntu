"""
Storage Module
==============

"""
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Union
from typing import Any
from typing import BinaryIO
from typing import Generator

from yuntu.core.utils.plugins import PluginMount


PathLike = Union[str, Path]


class Storage(metaclass=PluginMount):
    """
    Storage interface

    Storage objects are responsible for handling access and storage of files.
    They should know when a file is available for reading, how to access and
    retrieve the contents, and in most cases how to store data in files.

    The number of types of storages available can be expanded by using the
    plugin system.

    TODO: Finish
    """

    @abstractmethod
    def exists(self, path: PathLike):
        pass

    @abstractmethod
    @contextmanager
    def open(
        self,
        path: PathLike,
        mode: str = "rb",
    ) -> Generator[BinaryIO, None, None]:
        pass

    @abstractmethod
    def read(self, path: PathLike) -> bytes:
        pass

    @abstractmethod
    def write(self, path: PathLike, contents) -> None:
        pass

    @abstractmethod
    def size(self, path: PathLike) -> int:
        pass

    @classmethod
    @abstractmethod
    def is_compatible(self, path: PathLike) -> bool:
        return False

    @classmethod
    def get_default_config(cls) -> Any:
        return {}
