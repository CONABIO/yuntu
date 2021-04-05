import os
from urllib.parse import urlparse
from pathlib import Path
from contextlib import contextmanager

from yuntu.core.media.storage.base import Storage


class FileSystemStorage(Storage):
    @staticmethod
    def _parse_path(path):
        if isinstance(path, Path):
            return path

        return urlparse(path).path

    def exists(self, path):
        path = self._parse_path(path)
        return os.path.exists(path)

    @contextmanager
    def open(self, path, mode="rb"):
        path = self._parse_path(path)
        with open(path, mode) as fsfile:
            yield fsfile

    def write(self, path, contents):
        path = self._parse_path(path)
        with self.open(path, "wb") as fsfile:
            fsfile.write(contents)

    def read(self, path):
        path = self._parse_path(path)
        with self.open(path, "rb") as fsfile:
            return fsfile.read()

    def size(self, path):
        path = self._parse_path(path)
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
