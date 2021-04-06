import os
import io
import pytest

from yuntu.core.utils import cache


def test_cache_interface():
    assert hasattr(cache, "Cache")
    assert hasattr(cache.Cache, "plugins")

    abstract_methods = cache.Cache.__abstractmethods__
    assert "encode_value" in abstract_methods
    assert "decode_value" in abstract_methods


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

    memcache.remove_item("a")
    assert "a" not in memcache
    assert "b" in memcache
    assert memcache.size == 1

    memcache.clean()
    assert memcache.size == 0

    memcache["a"] = "a"
    memcache["b"] = "b"
    # Read value
    assert memcache["a"] == "a"
    memcache.remove_item("b")
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


@pytest.mark.parametrize(
    "cachetype",
    [
        cache.MemCache,
        cache.TmpFileCache,
    ],
)
def test_lrufy(cachetype):
    instance = cache.lrufy(cachetype)()

    instance["a"] = "value a"
    instance["b"] = "value b"

    # Read value
    assert instance["a"] == "value a"
    # Last viewed should be "a"
    instance.remove_one()
    assert "a" in instance
    assert "b" not in instance

    instance.clean()

    instance["a"] = "value a"
    instance["b"] = "value b"
    instance["c"] = "value c"
    assert instance["a"] == "value a"
    # Last viewed should be "a"
    instance.remove_one()
    assert "a" in instance
    assert "b" not in instance
    assert "c" in instance


@pytest.mark.parametrize(
    "cachetype",
    [
        cache.MemCache,
        cache.TmpFileCache,
    ],
)
def test_streamfy(cachetype):
    instance = cache.streamfy(cachetype)()

    a = io.BytesIO(b"content a")

    instance["a"] = a

    assert not isinstance(instance.retrieve_value("a"), io.BytesIO)
    assert isinstance(instance["a"], io.BytesIO)
    assert a.read() == instance["a"].read()


@pytest.mark.parametrize(
    "cachetype",
    [
        cache.MemCache,
        cache.TmpFileCache,
    ],
)
def test_streamfy_and_lrufy(cachetype):
    instance = cache.lrufy(cache.streamfy(cachetype))()

    instance["a"] = io.BytesIO(b"content a")
    instance["b"] = io.BytesIO(b"content b")
    instance["c"] = io.BytesIO(b"content c")

    assert instance["a"].read() == b"content a"

    instance.remove_one()
    assert "a" in instance
    assert "b" not in instance
    assert "c" in instance

    instance = cache.streamfy(cache.lrufy(cachetype))()

    instance["a"] = io.BytesIO(b"content a")
    instance["b"] = io.BytesIO(b"content b")
    instance["c"] = io.BytesIO(b"content c")

    assert instance["a"].read() == b"content a"

    instance.remove_one()
    assert "a" in instance
    assert "b" not in instance
    assert "c" in instance
