"""DataStore Module.

A datastore is a utility class meant to facilitate
the import of large volumes of recordings into a yuntu
collection.
"""
from abc import ABC
from abc import abstractmethod


class Datastore(ABC):
    @abstractmethod
    def iter(self):
        """Return an iterator of the datums to import."""

    @abstractmethod
    def prepare_row(self, row):
        """Prepare a datastore row for collection insertion."""

    def insert_into(self, collection):
        for row in self.iter():
            meta = self.prepare_row(row)
            collection.insert(meta)
