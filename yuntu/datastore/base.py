"""DataStore Module.

A datastore is a utility class meant to facilitate
the import of large volumes of recordings into a yuntu
collection.
"""
from abc import ABC
from abc import abstractmethod
import os
import pickle
from pony.orm import db_session
# import dill


class Datastore(ABC):
    _metadata = None
    base_dir = '.'

    def get_abspath(self, path):
        return os.path.join(self.base_dir, path)

    @abstractmethod
    def iter(self):
        """Return an iterator of the data to import."""

    @abstractmethod
    def iter_annotations(self, datum):
        """Return an iterator of the annotations of the corresponding datum."""
        return []

    @abstractmethod
    def prepare_datum(self, datum):
        """Prepare a datastore datum for collection insertion."""

    @abstractmethod
    def prepare_annotation(self, datum, annotation):
        """Prepare a datastore annotation for collection insertion."""

    @abstractmethod
    def get_metadata(self):
        """Return self's metadata"""

    def pickle(self):
        """Pickle instance."""
        return pickle.dumps(self)

    @property
    def metadata(self):
        """Datastore metadata"""
        if self._metadata is None:
            self._metadata = self.get_metadata()
        return self._metadata

    @db_session
    def create_datastore_record(self, collection):
        """Register this datastore into the collection."""
        return collection.db_manager.models.datastore(metadata=self.metadata)

    @db_session
    def insert_into(self, collection):
        datastore_record = self.create_datastore_record(collection)
        datastore_id = datastore_record.id

        recording_inserts = 0
        annotation_inserts = 0
        for datum in self.iter():
            meta = self.prepare_datum(datum)
            meta['path'] = self.get_abspath(meta['path'])
            meta['datastore'] = datastore_record
            recording = collection.insert(meta)[0]

            for annotation in self.iter_annotations(datum):
                annotation_meta = self.prepare_annotation(datum, annotation)
                annotation_meta['recording'] = recording
                collection.annotate([annotation_meta])
                annotation_inserts += 1

            recording_inserts += 1

        return datastore_id, recording_inserts, annotation_inserts
