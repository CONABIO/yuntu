import os
from tempfile import NamedTemporaryFile
from typing import Hashable

from yuntu.core.utils.cache.base import Cache


class TmpFileCache(Cache):
    """Temporary files cache.

    Items will be stored in temporary files. A timestamp at item storage or
    retrieval will be kept to implement a LRU cache. When cache is too large
    the object with the earliest timestamp will be discarded. Hence only the
    last recently used item will be discarded when needed.
    """

    def encode_value(self, value, encoding=None):
        """Store the value into a temporary file."""
        if not isinstance(value, bytes):
            if encoding is None:
                encoding = "utf-8"

            value = bytes(value, encoding=encoding)

        with NamedTemporaryFile(mode="wb", delete=False) as tmpfile:
            tmpfile.write(value)

            return {
                "path": tmpfile.name,
                "encoding": encoding,
            }

    def get_path(self, key: Hashable) -> str:
        """Retrieve the path of the stored file."""
        value = self.retrieve_value(key)
        return value["path"]

    def decode_value(self, value: Hashable) -> bytes:
        """Read the stored value from temporary file."""
        path = value["path"]
        encoding = value["encoding"]

        with open(path, "rb") as tmpfile:
            data = tmpfile.read()

        if encoding is None:
            return data

        return data.decode(encoding)

    def remove_item(self, key: Hashable) -> None:
        """Delete the temporary file and remove from cache"""
        path = self.get_path(key)
        os.remove(path)
        super().remove_item(key)
