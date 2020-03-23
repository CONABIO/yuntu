"""DataStore Module.

A datastore is a utility class meant to facilitate
the import of large volumes of recordings into a yuntu
collection.
"""
from abc import ABC
from abc import abstractmethod
import pickle
from pony.orm import db_session
# import dill


class Datastore(ABC):
    metadata = {}

    @abstractmethod
    def iter(self):
        """Return an iterator of the datums to import."""

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

    def pickle(self):
        """Pickle instance."""
        return pickle.dumps(self)

    def get_metadata(self):
        return self.metadata

    def create_datastore_record(self, collection):
        """Register this datastore into the collection."""
        metadata = self.get_metadata()
        pickle = self.pickle()
        return collection.db_manager.models.datastore(
            pickle=pickle,
            metadata=metadata)

    @db_session
    def insert_into(self, collection):
        datastore_record = self.create_datastore_record(collection)

        for datum in self.iter():
            meta = self.prepare_datum(datum)
            meta['datastore'] = datastore_record
            recording = collection.insert(meta)[0]

            for annotation in self.iter_annotations(datum):
                annotation_meta = self.prepare_annotation(datum, annotation)
                annotation_meta['recording'] = recording
                collection.annotate([annotation_meta])
