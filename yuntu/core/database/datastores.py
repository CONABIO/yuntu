"""Distinct types of datastores."""
from datetime import datetime
from pony.orm import Required
from yuntu.core.database.base import Datastore


class ForeignDb(Datastore):
    """Datastore that builds data from a foreign database."""

    host = Required(str)
    database = Required(str)
    query = Required(str)
    last_update = Required(datetime)


class Storage(Datastore):
    """Datastore builds from directory structure."""

    dir_path = Required(str)


class RemoteStorage(Storage):
    """Datastore that builds metadata from a remote storage."""

    url = Required(str)
