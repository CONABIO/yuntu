"""
Reader Module
==============

"""
import os
import mimetypes
from pathlib import Path
from typing import Union
from typing import Any
from typing import List
from typing import Optional
from typing import Type
from typing import BinaryIO
from abc import abstractmethod

import numpy as np
from yuntu.core.utils.plugins import PluginMount


__all__ = [
    "FileReader",
    "TextReader",
    "get_reader",
]


class FileReader(metaclass=PluginMount):
    mime_types: List[str] = []

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def read(self, fp: BinaryIO) -> Any:
        """Parse the contents of the file buffer"""

    @classmethod
    def is_compatible(cls, path: Union[str, Path]):
        mime_type, encoding = mimetypes.guess_type(path)
        return mime_type in cls.mime_types

    @classmethod
    def get_default_config(cls):
        return {}


def get_reader(
    path: Union[str, Path],
    config: Optional[Any] = None,
) -> FileReader:
    reader_type = infer_reader_type(path)

    if config is None:
        config = reader_type.get_default_config()

    return reader_type(**config)


def infer_reader_type(path: Union[str, Path]) -> Type[FileReader]:
    apt_readers = [
        reader for reader in FileReader.plugins if reader.is_compatible(path)
    ]

    if len(apt_readers) == 0:
        message = "This file cannot be handled by the installed reader types"
        raise NotImplementedError(message)

    # Return the last reader class. This way newly installed readers can
    # overwrite behaviour.
    return apt_readers[-1]


class TextReader(FileReader):
    mime_types: List[str] = ["text/plain", "text/markdown"]

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def read(self, fp: BinaryIO) -> str:
        return fp.read().decode(self.encoding)


class NumpyReader(FileReader):
    @classmethod
    def is_compatible(cls, path: Union[str, Path]):
        _, extension = os.path.splitext(path)
        return extension == "npy"

    def read(self, fp: BinaryIO) -> str:
        return np.load(fp)
