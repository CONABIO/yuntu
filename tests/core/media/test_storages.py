import pytest
from yuntu.core.media.storage import Storage
from yuntu.core.media.storage import FileSystemStorage


def test_storage_interface():
    assert hasattr(Storage, "plugins")

    abstract_methods = [
        "open",
        "write",
        "size",
        "compatible_file",
    ]

    for method in abstract_methods:
        assert hasattr(Storage, method)

        class IncompleteStorage(Storage):
            pass

        for other_method in abstract_methods:
            if other_method == method:
                continue

            setattr(IncompleteStorage, other_method, lambda self, path: None)

        with pytest.raises(TypeError):
            IncompleteStorage()


def test_default_storage():
    assert isinstance(Storage.get_default(), FileSystemStorage)

    path = "sample_file.txt"

    assert isinstance(Storage.get_storage(path), FileSystemStorage)
    assert Storage.get_storage(path) is Storage.get_default()
