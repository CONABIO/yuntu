"""
Cache System

Yuntu provides a caching system to avoid repeating expensive operations, such
as a complicated computation or a file download. The cache interface is
specified by the :class:`Cache` class.

The base package has two cache implementations:

    1. :class:`MemCache`
    2. :class:`TmpFileCache`

.. doctest::

    >>> from yuntu.core.utils.cache import Cache, MemCache, TmpFileCache

    >>> MemCache in Cache.plugins
    True

    >>> TmpFileCache in Cache.plugins
    True

    >>> len(Cache.plugins)
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

        @cache()
        def func(*args):
            ...
            return value

        # The first call of the function will evaluate it
        val = func(arg1, arg2)

        # Any further function call with the same arguments will retrieve the
        # output from the cache. This should be faster a second time
        val = func(arg1, arg2)

"""
from yuntu.core.utils.cache.base import Cache
from yuntu.core.utils.cache.tmpfile import TmpFileCache
from yuntu.core.utils.cache.memcache import MemCache
from yuntu.core.utils.cache.mixins import BytesIOCacheMixin
from yuntu.core.utils.cache.mixins import LRUCacheMixin


__all__ = [
    "cache",
    "TmpFileCache",
    "TMP_FILE_CACHE",
    "MEM_CACHE",
]


TMP_FILE_CACHE = TmpFileCache.__name__

MEM_CACHE = MemCache.__name__


def cache(name: str, lru=True, stream=False, max_size=None, **kwargs) -> Cache:
    """Build a cache of the desired type.

    Args:
        name (str): The name of the cache system to build.
        lru (bool, optional): If true, the cache will use the LRU algorithm
            to dispose unwanted items. Defaults to True.
        stream (bool, optional): If True the cache system will only accept
            and return BytesIO objects, default to False.
        max_size (int, optional): The maximum number of items the cache can
            store. If None the cache does not have a maximum size. Defaults
            to None.
        **kwargs: Additional configuration arguments for the cache.

    Example:
        To build an in-memory cache object

        .. doctest::

            >>> from yuntu.core.utils.cache import cache

            >>> c = cache('MemCache', max_size=10)
            >>> key = "a"
            >>> value = 8238
            >>> c[key] = value

            >>> key in c
            True

            >>> c[key] == value
            True

            >>> c.size
            1

    Returns:
        Cache: The cache instance

    Raises:
        NotImplementedError: If no cache implementation exists with
            the given name.

    """
    for implementation in Cache.plugins:
        if implementation.__name__ == name:
            klass = implementation
            break

    else:
        message = f"No cache implementation with name {name} was found"
        raise NotImplementedError(message)

    if lru:
        klass = lrufy(klass)

    if stream:
        klass = streamfy(klass)

    return klass(**kwargs)


def streamfy(klass):
    return klass

    class WrappedClass(BytesIOCacheMixin, klass):
        pass

    return WrappedClass


def lrufy(klass):
    return klass

    class WrappedClass(LRUCacheMixin, klass):
        pass

    return WrappedClass
