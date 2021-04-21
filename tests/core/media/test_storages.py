import os
import io
import requests
from urllib.parse import urlparse

import pytest

from yuntu.core.media import storage


@pytest.fixture(autouse=True)
def flush():
    storage.Storage.flush()
    storage.Storage.plugins.append(storage.FileSystemStorage)


def test_storage_interface():
    assert hasattr(storage.Storage, "plugins")

    abstract_methods = [
        "open",
        "write",
        "size",
        "read",
    ]

    for method in abstract_methods:
        assert hasattr(storage.Storage, method)

        class IncompleteStorage(storage.Storage):
            @classmethod
            def is_compatible(cls, path):
                return False

        for other_method in abstract_methods:
            if other_method == method:
                continue

            setattr(IncompleteStorage, other_method, lambda self, path: None)

        with pytest.raises(TypeError):
            IncompleteStorage()


def test_default_storage():
    path = "sample_file.txt"

    assert isinstance(storage.get_default(), storage.FileSystemStorage)
    assert isinstance(storage.get_storage(path), storage.FileSystemStorage)
    assert storage.get_storage(path) is storage.get_default()


def test_infer_storage_type():
    file_path = "file:///path"
    http_path = "https://host/path/"
    s3_path = "s3://host/path/"

    assert storage.infer_storage_type(file_path) is storage.FileSystemStorage

    with pytest.raises(NotImplementedError):
        storage.infer_storage_type(http_path)

    with pytest.raises(NotImplementedError):
        storage.infer_storage_type(s3_path)

    class HttpDummyStorage(storage.FileSystemStorage):
        @classmethod
        def is_compatible(cls, path):
            parsed = urlparse(path)
            return parsed.scheme in ["http", "https"]

    assert storage.infer_storage_type(file_path) is storage.FileSystemStorage
    assert storage.infer_storage_type(http_path) is HttpDummyStorage

    with pytest.raises(NotImplementedError):
        storage.infer_storage_type(s3_path)

    class S3DummyStorage(storage.FileSystemStorage):
        @classmethod
        def is_compatible(cls, path):
            parsed = urlparse(path)
            return parsed.scheme == "s3"

    assert storage.infer_storage_type(file_path) is storage.FileSystemStorage
    assert storage.infer_storage_type(http_path) is HttpDummyStorage
    assert storage.infer_storage_type(s3_path) is S3DummyStorage


def test_get_storage():
    file_path = "file:///path"
    http_path = "https://host/path/"
    s3_path = "s3://host/path/"

    assert isinstance(
        storage.get_storage(file_path),
        storage.FileSystemStorage,
    )

    with pytest.raises(NotImplementedError):
        storage.get_storage(http_path)

    with pytest.raises(NotImplementedError):
        storage.get_storage(s3_path)

    class HttpDummyStorage(storage.FileSystemStorage):
        @classmethod
        def is_compatible(cls, path):
            parsed = urlparse(path)
            return parsed.scheme in ["http", "https"]

    assert isinstance(
        storage.get_storage(file_path),
        storage.FileSystemStorage,
    )
    assert isinstance(storage.get_storage(http_path), HttpDummyStorage)

    with pytest.raises(NotImplementedError):
        storage.get_storage(s3_path)

    class S3DummyStorage(storage.FileSystemStorage):
        @classmethod
        def is_compatible(cls, path):
            parsed = urlparse(path)
            return parsed.scheme == "s3"

    assert isinstance(
        storage.get_storage(file_path),
        storage.FileSystemStorage,
    )
    assert isinstance(storage.get_storage(http_path), HttpDummyStorage)
    assert isinstance(storage.get_storage(s3_path), S3DummyStorage)


def test_set_default():
    assert isinstance(storage.get_default(), storage.FileSystemStorage)

    class DummyStorage(storage.FileSystemStorage):
        @classmethod
        def is_compatible(cls, path):
            parsed = urlparse(path)
            return parsed.scheme == "dummy"

    with pytest.raises(ValueError):
        storage.set_default(DummyStorage)

    storage.set_default(DummyStorage())

    assert isinstance(storage.get_default(), DummyStorage)

    storage.reset_default()

    assert isinstance(storage.get_default(), storage.FileSystemStorage)


@pytest.mark.parametrize(
    "converter",
    [
        lambda x: x,
        lambda x: x.as_posix(),
        lambda x: x.as_uri(),
    ],
)
def tests_filesystem_storage(tmp_path, converter):
    inexistent_path = tmp_path / "inexistent"
    empty_path = tmp_path / "empty"
    non_empty_path = tmp_path / "non_empty"

    empty_path.touch()
    content = b"non_empty"
    non_empty_path.write_bytes(content)

    inexistent_path = converter(inexistent_path)
    empty_path = converter(empty_path)
    non_empty_path = converter(non_empty_path)

    st = storage.FileSystemStorage()

    # Tests exists
    assert not st.exists(inexistent_path)
    assert st.exists(empty_path)
    assert st.exists(non_empty_path)

    # Test open
    with pytest.raises(IOError):
        with st.open(inexistent_path):
            pass

    with st.open(empty_path) as fsfile:
        assert isinstance(fsfile, io.BufferedReader)

    with st.open(non_empty_path) as fsfile:
        assert isinstance(fsfile, io.BufferedReader)

    # Test read
    with pytest.raises(IOError):
        st.read(inexistent_path)

    assert not st.read(empty_path)
    assert content == st.read(non_empty_path)

    # Test size
    with pytest.raises(IOError):
        st.size(inexistent_path)

    assert st.size(empty_path) == 0
    assert st.size(non_empty_path) == len(content)

    # Test write
    st.write(inexistent_path, content)
    assert st.read(inexistent_path) == content


@pytest.mark.parametrize(
    "url",
    [
        "https://homepages.cae.wisc.edu/~ece533/images/baboon.png",
        "https://filesamples.com/samples/image/bmp/sample_640%C3%97426.bmp",
        #  "https://filesamples.com/samples/audio/wav/sample3.wav",
        "https://filesamples.com/samples/document/csv/sample4.csv",
        "https://filesamples.com/samples/document/txt/sample2.txt",
        "https://filesamples.com/samples/document/pdf/sample2.pdf",
        "https://filesamples.com/samples/image/jpeg/sample_640%C3%97426.jpeg",
    ],
)
def test_http_storage(url, monkeypatch):
    st = storage.HTTPStorage()
    assert st.is_compatible(url)
    assert st.exists(url)

    st.download(url)
    assert os.path.exists(st._cache.get_path((url,)))

    def mockget(*args, **kwargs):
        # Should not request a second time
        raise AssertionError

    monkeypatch.setattr(requests, "get", mockget)
    st.download(url)

    # Check the file was not deleted
    assert os.path.exists(st._cache.get_path((url,)))

    with pytest.raises(NotImplementedError):
        st.write(url, b"cant write")

    with st.open(url) as f:
        assert isinstance(f, io.BytesIO)


def test_http_storage_not_exists():
    st = storage.HTTPStorage()

    inexistent = [
        "http://does.not.exists/i/dont/exists.txt",
        "http://my.existence.is/not/real.png",
        "https://iam.aghost.com/just/imagination.csv",
    ]

    for path in inexistent:
        assert not st.exists(path)
