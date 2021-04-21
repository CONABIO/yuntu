import os
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO
from typing import Generator
from urllib.parse import urlparse

from yuntu.core.media.storage.base import Storage
from yuntu.core.media.storage.base import PathLike


class FileSystemStorage(Storage):
    @staticmethod
    def _parse_path(path: PathLike) -> PathLike:
        if isinstance(path, Path):
            return path

        return urlparse(path).path

    def exists(self, path: PathLike) -> bool:
        path = self._parse_path(path)
        return os.path.exists(path)

    @contextmanager
    def open(self, path, mode="rb") -> Generator[BinaryIO, None, None]:
        path = self._parse_path(path)
        with open(path, mode) as fsfile:
            yield fsfile

    def write(self, path: PathLike, contents: bytes):
        path = self._parse_path(path)
        with self.open(path, "wb") as fsfile:
            fsfile.write(contents)

    def read(self, path: PathLike) -> bytes:
        path = self._parse_path(path)
        with self.open(path, "rb") as fsfile:
            return fsfile.read()

    def size(self, path: PathLike) -> int:
        path = self._parse_path(path)
        return os.path.getsize(path)

    @classmethod
    def is_compatible(cls, path: PathLike) -> bool:
        if isinstance(path, Path):
            path = str(path.resolve())

        parsed = urlparse(path)

        if parsed.scheme == "file":
            return True

        if parsed.scheme:
            # For every other scheme return False
            return False

        return True
