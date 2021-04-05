from urllib.parse import urlparse
from contextlib import contextmanager

import requests

from yuntu.core.media.storage.base import Storage


class HTTPStorage(Storage):
    def download(self, path):
        pass

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
    def open(self, path, mode="rb"):
        pass

    def write(self, path, contents):
        pass

    def read(self, path):
        pass

    def size(self, path):
        pass

    @classmethod
    def compatible_file(cls, path):
        parsed = urlparse(path)
        return parsed.scheme in ["http", "https"]
