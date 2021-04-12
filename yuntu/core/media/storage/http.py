from io import BytesIO
from urllib.parse import urlparse
from contextlib import contextmanager

import requests

from yuntu.core.media.storage.base import Storage

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
    def download(self, path):
        request = requests.get(path)

        if request.status_code != 200:
            message = "An error ocurred during the download"
            raise requests.exceptions.HTTPError(message)

        return request.content

    def exists(self, path):
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
    def open(self, path):
        downloaded = self.download(path)

        with BytesIO(downloaded) as f:
            yield f

    def write(self, path, contents):
        message = "The HTTPStorage has no write method"
        raise NotImplementedError(message)

    def read(self, path):
        with self.open(path) as f:
            return f.read()

    def size(self, path):
        downloaded = self.download(path)
        return len(downloaded)

    @classmethod
    def is_compatible(cls, path):
        parsed = urlparse(path)
        return parsed.scheme in ["http", "https"]
