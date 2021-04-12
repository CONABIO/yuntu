"""
In-Memory Cache

This cache system will store the values, uncompressed, into memory.
"""
from typing import Any

from yuntu.core.utils.cache.base import Cache


class MemCache(Cache):
    """In-memory Cache.

    Items will be stored in memory together with a timestamp of the time at
    storage. If an object is retrieved after storing, its timestamp will be
    updated. When cache is too large the object with the earliest timestamp
    will be discarded. Hence only the last recently used item will be
    discarded.
    """

    def encode_value(self, value: Any) -> Any:
        """Store the values into memory unchanged."""
        return value

    def decode_value(self, value: Any) -> Any:
        """Retrieve values from memory without modifications."""
        return value
