from pathlib import Path
from abc import ABC
from typing import Union
from typing import Optional
from typing import Any

from yuntu.core.utils.lazy import lazy_property
from yuntu.core.media.storage import get_storage
from yuntu.core.media.storage import Storage
from yuntu.core.media.readers import get_reader
from yuntu.core.media.readers import FileReader
from yuntu.core.media.info import get_info_reader
from yuntu.core.media.info import MediaInfoReader


class Media(ABC):
    """
    Media objects.

    A media object represents a either the contents of a file or
    a object that could be stored in a file.
    """

    media_info_type: Optional[str] = None

    def __init__(
        self,
        path: Optional[Union[Path, str]] = None,
        content: Optional[Any] = None,
        storage: Optional[Storage] = None,
        reader: Optional[FileReader] = None,
        media_info_reader: Optional[MediaInfoReader] = None,
        media_info_type: Optional[str] = None,
    ):
        if path is None and content is None:
            message = "A path or the content must be provided"
            raise ValueError(message)

        self.path = path
        self.storage = storage
        self.reader = reader
        self.media_info_reader = media_info_reader

        if media_info_type is not None:
            self.media_info_type = media_info_type

        if content is not None:
            self.content = content

    @lazy_property
    def bytes(self) -> bytes:
        if self.path is None:
            raise ValueError

        storage = self.storage

        if storage is None:
            storage = get_storage(self.path)

        return storage.read(self.path)

    @lazy_property
    def content(self) -> Any:
        if self.path is None:
            raise ValueError

        storage = self.storage

        if storage is None:
            storage = get_storage(self.path)

        reader = self.reader

        if reader is None:
            reader = get_reader(self.path)

        with storage.open(self.path) as fp:
            return reader.read(fp)

    @lazy_property
    def media_info(self):
        storage = self.storage

        if storage is None:
            storage = get_storage(self.path)

        reader = self.media_info_reader

        if reader is None:
            reader = get_info_reader(self.path, self.media_info_type)

        with storage.open(self.path) as fp:
            return reader.read(fp)
