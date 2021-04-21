from io import BytesIO
from pathlib import Path
from typing import Generator
from typing import BinaryIO
from urllib.parse import urlparse
from contextlib import contextmanager

import requests

from yuntu.core.media.storage.base import Storage
from yuntu.core.media.storage.base import PathLike
from yuntu.core.utils.cache import cache
from yuntu.core.utils.cache import TMP_FILE_CACHE


def get_http_cache_config():
    return {
        "name": TMP_FILE_CACHE,
        "lru": True,
        "max_size": 100,
    }


class HTTPStorage(Storage):
    _cache = cache(**get_http_cache_config())

    @_cache(method=True)
    def download(self, path: PathLike) -> bytes:
        if isinstance(path, Path):
            path = str(path.resolve())

        request = requests.get(path)

        if request.status_code != 200:
            message = "An error ocurred during the download"
            raise requests.exceptions.HTTPError(message)

        return request.content

    def exists(self, path: PathLike) -> bool:
        if isinstance(path, Path):
            path = str(path.resolve())

        try:
            request = requests.head(path, allow_redirects=False)

            if request.status_code != 200:
                return False

            if "Connection" in request.headers:
                if request.headers["Connection"] == "close":
                    return False

            return True

        except requests.exceptions.ConnectionError:
            return False

    @contextmanager
    def open(
        self,
        path: PathLike,
        mode="rb",
    ) -> Generator[BinaryIO, None, None]:
        downloaded = self.download(path)

        with BytesIO(downloaded) as f:
            yield f

    def write(self, path, contents):
        message = "The HTTPStorage has no write method"
        raise NotImplementedError(message)

    def read(self, path: PathLike) -> bytes:
        with self.open(path) as f:
            return f.read()

    def size(self, path: PathLike) -> int:
        downloaded = self.download(path)
        return len(downloaded)

    @classmethod
    def is_compatible(cls, path: PathLike) -> bool:
        if isinstance(path, Path):
            path = str(path.resolve())

        parsed = urlparse(path)
        return parsed.scheme in ["http", "https"]
