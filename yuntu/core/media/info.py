from pathlib import Path
from typing import Union
from typing import Any
from typing import List
from typing import Optional
from typing import Type
from typing import BinaryIO
from abc import abstractmethod

from yuntu.core.utils.plugins import PluginMount


__all__ = [
    "MediaInfoReader",
    "get_info_reader",
]


class MediaInfoReader(metaclass=PluginMount):
    media_info_types: List[str] = []

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def read(self, fp: BinaryIO) -> Any:
        pass
        """Parse the contents of the file buffer"""

    @classmethod
    def is_compatible(cls, path: Union[str, Path]) -> bool:
        return True

    @classmethod
    def get_default_config(cls):
        return {}


def get_info_reader(
    path: Union[str, Path],
    media_info_type: Optional[str] = None,
    config: Optional[Any] = None,
) -> MediaInfoReader:
    reader_type = infer_info_reader_type(path, media_info_type=media_info_type)

    if config is None:
        config = reader_type.get_default_config()

    return reader_type(**config)


def infer_info_reader_type(
    path: Union[str, Path],
    media_info_type: Optional[str] = None,
) -> Type[MediaInfoReader]:
    apt_readers = [
        reader
        for reader in MediaInfoReader.plugins
        if reader.is_compatible(path)
    ]

    if media_info_type is not None:
        apt_readers = [
            reader
            for reader in apt_readers
            if media_info_type in reader.media_info_types
        ]

    if len(apt_readers) == 0:
        message = "The file info cannot be read by the installed reader types"
        raise NotImplementedError(message)

    # Return the last reader class. This way newly installed readers can
    # overwrite behaviour.
    return apt_readers[-1]
