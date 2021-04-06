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
from .base import Cache
from .memcache import MemCache
from .tmpfile import TmpFileCache

from .mixins import BytesIOCacheMixin
from .mixins import LRUCacheMixin


def streamfy(klass):
    class WrappedClass(BytesIOCacheMixin, klass):
        pass

    return WrappedClass


def lrufy(klass):
    class WrappedClass(LRUCacheMixin, klass):
        pass

    return WrappedClass


__all__ = [
    "Cache",
    "MemCache",
    "TmpFileCache",
    "streamify",
    "lruify",
]
