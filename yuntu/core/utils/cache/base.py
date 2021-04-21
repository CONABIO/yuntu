"""
The Cache Interface

All cache objects keep an internal dictionary to track stored values::

    cache = Cache()

    # Internal dictionary
    cache._cache

Naively, storing a key-value pair in the cache could be achieved by storing
them directly in this internal dictionary::

    # Storing a key, value pair
    cache._cache[key] = value

    # Retrieveing a value by key
    value = cache._cache[key]

To add flexibility to the cache system we instead store an encoded version of
the value, akin to this::

    # Storing a key, value pair
    cache._cache[key] = cache.encode_value(value)

    # retrieving a value by key
    value = cache.decode_value(cache._cache[key])

The encoded representation of the value can be anything as long as the
original value can be reconstructed from the representation. It can be the
full object itself, the path to the file where it was stored, etc.
"""
from abc import abstractmethod
from typing import Any
from typing import Hashable
from typing import Optional
from functools import wraps

from yuntu.core.utils.plugins import PluginMount


class Cache(metaclass=PluginMount):
    """Base Cache Interface

    All cache implementations should inherit from this class and overwrite the
    methods :meth:`Cache.encode_value` and :meth:`Cache.decode_value`.
    """

    def __init__(self, max_size: Optional[int] = None):
        self._cache = {}
        self.max_size = max_size

    def __contains__(self, key: Hashable) -> bool:
        return self.has(key)

    def __getitem__(self, key: Hashable) -> Any:
        return self.get(key)

    def __setitem__(self, key: Hashable, value: Any) -> None:
        self.update(key, value)

    def __call__(self, method=False):
        """Decorator to store function outputs in the cache.

        Example:
            You can use the decorator when you have a computational expensive
            function::

                cache = MemCache()

                @cache()
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

        def decorator(func):
            @wraps(func)
            def wrapper(*args):
                if method:
                    key = args[1:]

                else:
                    key = args

                if key in self:
                    return self[key]

                value = func(*args)
                self[key] = value
                return value

            return wrapper

        return decorator

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

        self._cache[key] = self.encode_value(value)

    def get(self, key: Hashable) -> Any:
        """Get an item from the cache.

        To retrieve an object from the cache you can either::

            value = cache.get(key)

        or::

            value = cache[key]
        """
        return self.decode_value(self.retrieve_value(key))

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
        if self.max_size is None:
            return False

        return self.size >= self.max_size

    def remove_one(self) -> None:
        """Select and remove an item from the cache."""
        self._cache.popitem()

    def remove_item(self, key: Hashable) -> None:
        del self._cache[key]

    def retrieve_value(self, key: Hashable) -> Any:
        """Get the stored encoded value"""
        return self._cache[key]

    @abstractmethod
    def encode_value(self, value: Any) -> Any:
        """Encode the information to be stored.

        The original value should be reconstructible from the stored value.
        """

    @abstractmethod
    def decode_value(self, value: Any) -> Any:
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
