"""
Yuntu Cache System
==================

Yuntu provides a caching system to avoid repeating expensive operations, such
as a complicated computation or a file download. The cache interface is
specified by the Cache class.

The base package has two cache implementations:

    1. :class:`MemCache`
    2. :class:`TmpFileCache`

.. doctest::

    >> MemCache in Cache.plugins
    True

    >> TmpFileCache in Cache.plugins
    True

    >> len(Cache.plugins)
    2

The user interface is identical.

Example:
    Say you wish to use a cache of memory stored items, but to avoid occupying
    to much memory you limit the number of items it can store to 10::

        cache = MemCache(max_size=10)

        # Or
        cache = TmpFileCache(max_size=10)

    To store a key, value pair::

        cache[key] = value

        # The following is equivalent
        cache.update(key, value)


    To retrieve a stored value::

        value = cache[key]

        # Or, equivalently
        value = cache.get(key)

    To remove a single entry::

        # If you need release space from the cache but don't care
        # which item is discarded
        cache.remove_one()

        # Or if you want to remove a selected item
        cache.remove_item(key)

    To delete all entries in the cache::

        cache.clean()

    If you wish to use the cache to store the outputs of function, you can use
    the cache as a decorator::

        @cache
        def func(*args):
            ...
            return value

        # The first call of the function will evaluate it
        val = func(arg1, arg2)

        # Any further function call with the same arguments will retrieve the
        # output from the cache. This should be faster a second time
        val = func(arg1, arg2)

"""
import os
import datetime
from tempfile import NamedTemporaryFile
from abc import abstractmethod
from typing import Hashable
from typing import Optional
from typing import TypedDict
from typing import Any
from functools import wraps

from yuntu.core.utils.plugins import PluginMount


class Cache(metaclass=PluginMount):
    """Base Cache Interface

    All cache implementations should inherit from this class. The
    implementations need to overwrite the methods :meth:`Cache.remove_one`,
    :meth:`Cache.store_value` and :meth:`Cache.retrieve_value`
    """

    def __init__(self):
        self._cache = {}

    def __contains__(self, key: Hashable) -> bool:
        return self.has(key)

    def __getitem__(self, key: Hashable) -> Any:
        return self.get(key)

    def __setitem__(self, key: Hashable, value: Any) -> None:
        self.update(key, value)

    def __call__(self, func):
        """Decorator to store function outputs in the cache.

        Example:
            You can use the decorator when you have a computational expensive
            function::

                cache = MemCache()

                @cache.cache
                def computationally_expensive(arg1, arg2):
                    ...
                    return value

            The first time the function is called, the function will be
            computed and stored in the cache. The next time the function is
            called with the same arguments instead of computing the function
            again, the value will be retrieved from the cache::

                # Computed the first time
                value = computationally_expensive(arg1, arg2)

                # Second time should be very fast since it's only
                # retrieving from the cache
                value = computationally_expensive(arg1, arg2)

                # Calling the same function with other arguments will
                # invoke the function a second time
                value2 = computationally_expensive(arg3, arg4)

        """

        @wraps(func)
        def wrapper(*args):
            key = tuple(args)
            if key in self:
                return self[key]

            value = func(*args)
            self[key] = value
            return value

        return wrapper

    def update(self, key: Hashable, value: Any) -> None:
        """
        Add an item to the cache.

        To store an object in the cache you can either::

            cache.update(key, value)

        or::

            cache[key] = value

        """
        if key not in self and self.too_large():
            self.remove_one()

        self._cache[key] = self.store_value(value)

    def get(self, key: Hashable) -> Any:
        """Get an item from the cache.

        To retrieve an object from the cache you can either::

            value = cache.get(key)

        or::

            value = cache[key]
        """
        return self.retrieve_value(self._cache[key])

    def has(self, key: Hashable) -> bool:
        """Check if cache has object.

        To see if an item is stored in the cache you can either::

            cache.has(key)

        or::

            key in cache
        """
        return key in self._cache

    def too_large(self) -> bool:
        """Determine if the cache has too many items."""
        return False

    @abstractmethod
    def remove_one(self) -> None:
        """Select and remove an item from the cache."""

    def remove_item(self, key: Hashable) -> None:
        del self._cache[key]

    @abstractmethod
    def store_value(self, value: Any) -> Any:
        """Transform the object into a stored value.

        The original value should be reconstructible from the stored value.
        """

    @abstractmethod
    def retrieve_value(self, value: Any) -> Any:
        """Reconstruct the original value from the stored value."""

    @property
    def size(self) -> int:
        """The number of items stored in the cache."""
        return len(self._cache)

    def clean(self) -> None:
        """Remove all items from the cache."""
        keys = list(self._cache.keys())
        for key in keys:
            self.remove_item(key)


MemStoredItem = TypedDict(
    "MemStoredItem",
    {"date": datetime.datetime, "value": Any},
)


class LRUCacheMixin:
    """Least Recently Used Mixin.

    This helper class implements the least recently used algorithm for
    cache management. Cache systems inheriting from this class will select
    the oldest item in the cache for removal. If an item is retrieved from
    the cache it will be considered new.
    """

    def __init__(self, max_size: Optional[int] = 10):
        """Initialize Cache for storage.

        Args:
            max_size (int, optional): The maximum number of items this cache
            can store. If None, the cache size has no limit.
        """
        super().__init__()
        self.max_size = max_size

    def too_large(self) -> bool:
        """Check if the cache exceeds the maximum amount of storage slots"""
        if self.max_size is None:
            return False

        return self.size >= self.max_size

    def store_value(self, value: Any) -> MemStoredItem:
        """Save the value and storage date in memory."""
        return {
            "date": datetime.datetime.now(),
            "value": value,
        }

    def retrieve_value(self, value: MemStoredItem) -> Any:
        """Retrieve the value from memory"""
        value["date"] = datetime.datetime.now()
        return value["value"]

    def remove_one(self) -> None:
        """Remove the last updated stored item."""
        last = min(
            self._cache,
            key=lambda k: self._cache[k]["date"],
        )

        self.remove_item(last)


class MemCache(LRUCacheMixin, Cache):
    """In memory cache.

    Items will be stored in memory together with a timestamp of the time at
    storage. If an object is retrieved after storing, its timestamp will be
    updated. When cache is too large the object with the earliest timestamp
    will be discarded. Hence only the last recently used item will be
    discarded.
    """


class TmpFileCache(LRUCacheMixin, Cache):
    """Temporary files cache.

    Items will be stored in temporary files. A timestamp at item storage or
    retrieval will be kept to implement a LRU cache. When cache is too large
    the object with the earliest timestamp will be discarded. Hence only the
    last recently used item will be discarded when needed.
    """

    def store_value(self, value):
        """Store the value into a temporary file."""
        with NamedTemporaryFile(mode="wb", delete=False) as tmpfile:
            tmpfile.write(value)
            return super().store_value(tmpfile.name)

    def get_path(self, key: Hashable) -> str:
        """Retrieve the path of the stored file."""
        return super().retrieve_value(self._cache[key])

    def retrieve_value(self, value: Hashable) -> bytes:
        """Read the stored value from temporary file."""
        path = super().retrieve_value(value)

        with open(path, "rb") as tmpfile:
            return tmpfile.read()

    def remove_item(self, key: Hashable) -> None:
        """Delete the temporary file and remove from cache"""
        path = self.get_path(key)
        os.remove(path)
        super().remove_item(key)
