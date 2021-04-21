from pathlib import Path
from typing import Union
from typing import Type

from yuntu.core.media.storage.base import Storage
from yuntu.core.media.storage.filesystem import FileSystemStorage
from yuntu.core.media.storage.http import HTTPStorage


__all__ = [
    "Storage",
    "FileSystemStorage",
    "HTTPStorage",
    "get_storage",
]


_default = None


def get_storage(path: Union[str, Path], config=None) -> Storage:
    if isinstance(path, Path):
        path = str(path.resolve())

    default = get_default()

    if default.is_compatible(path):
        return default

    storage_type = infer_storage_type(path)

    if config is None:
        config = storage_type.get_default_config()

    return storage_type(**config)


def infer_storage_type(path: str) -> Type[Storage]:
    apt_storages = [
        storage for storage in Storage.plugins if storage.is_compatible(path)
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
