import os

from yuntu.core.utils import cache


def test_cache_interface():
    assert hasattr(cache, "Cache")
    assert hasattr(cache.Cache, "plugins")

    abstract_methods = cache.Cache.__abstractmethods__
    assert "remove_one" in abstract_methods
    assert "store_value" in abstract_methods
    assert "retrieve_value" in abstract_methods


def test_memcache_too_large():
    max_size = 10
    memcache = cache.MemCache(max_size=max_size)

    for num in range(20):
        if num < max_size:
            assert not memcache.too_large()

        else:
            assert memcache.too_large()

        memcache[num] = num
        assert memcache.size <= max_size


def test_memcache_remove_one():
    memcache = cache.MemCache(max_size=10)

    memcache["a"] = "a"
    assert "a" in memcache
    assert "b" not in memcache
    assert memcache.size == 1

    memcache.remove_one()
    assert "a" not in memcache
    assert memcache.size == 0

    memcache["a"] = "a"
    memcache["b"] = "b"
    assert "a" in memcache
    assert "b" in memcache
    assert memcache.size == 2

    memcache.remove_one()
    assert "a" not in memcache
    assert "b" in memcache
    assert memcache.size == 1

    memcache.clean()
    assert memcache.size == 0

    memcache["a"] = "a"
    memcache["b"] = "b"
    # Read value
    assert memcache["a"] == "a"
    memcache.remove_one()
    # Last viewed should be "a"
    assert "a" in memcache
    assert "b" not in memcache


def test_tmpfile_cache():
    tmpcache = cache.TmpFileCache()

    content = b"I am the content of a cached file"
    key = "please cache this"

    tmpcache[key] = content
    path = tmpcache.get_path(key)

    assert isinstance(path, str)
    assert os.path.exists(path)
    assert path != content
    assert content == tmpcache[key]

    tmpcache.remove_item(key)
    assert not os.path.exists(path)
    assert key not in tmpcache
    assert tmpcache.size == 0
