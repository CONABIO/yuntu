from .base import Storage
from .filesystem import FileSystemStorage
from .http import HTTPStorage


_default = None


def get_storage(path, config=None):
    default = get_default()

    if default.compatible_file(path):
        return default

    storage_type = infer_storage_type(path)

    if config is None:
        config = storage_type.get_default_config()

    return storage_type(**config)


def infer_storage_type(path):
    apt_storages = [
        storage for storage in Storage.plugins if storage.compatible_file(path)
    ]

    if len(apt_storages) == 0:
        message = "This file cannot be handled by the installed storage types"
        raise NotImplementedError(message)

    # Return the last storage class. This way newly installed storages
    # can overwrite behaviour.
    return apt_storages[-1]


def get_default():
    global _default

    if _default is None:
        set_default(FileSystemStorage())

    return _default


def reset_default():
    set_default(FileSystemStorage())


def set_default(storage):
    global _default

    if not isinstance(storage, Storage):
        raise ValueError

    _default = storage


__all__ = [
    "Storage",
    "FileSystemStorage",
    "HTTPStorage",
    "get_storage",
]
