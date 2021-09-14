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


class Datastore(ABC):

    def __init__(self):
        self._metadata = None
        self.base_dir = '.'

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

    def create_datastore_record(self, collection):
        """Register this datastore into the collection."""
        return collection.db_manager.models.datastore(metadata=self.metadata)

    @db_session
    def insert_into(self, collection):
        datastore_record = self.create_datastore_record(collection)
        datastore_record.flush()
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


class Storage(Datastore, ABC):

    def __init__(self, dir_path):
        super().__init__()
        self.dir_path = dir_path

    def create_datastore_record(self, collection):
        """Register this datastore into the collection."""
        return collection.db_manager.models.storage(metadata=self.metadata,
                                                    dir_path=self.dir_path)


class RemoteStorage(Storage, ABC):

    def __init__(self, dir_path, metadata_url=None, auth=None):
        super().__init__(dir_path)
        self.metadata_url = metadata_url
        self.auth = auth

    def create_datastore_record(self, collection):
        """Register this datastore into the collection."""
        return collection.db_manager.models.remote_storage(metadata=self.metadata,
                                                           dir_path=self.dir_path)

class DataBaseDatastore(Datastore, ABC):

    def __init__(self, db_config, query, mapping, base_dir=None, tqdm=None):
        super().__init__()
        self.db_config = db_config
        self.query = query

        if base_dir is not None:
            self.base_dir = base_dir

        self.mapping = mapping
        self.tqdm = tqdm

    def get_metadata(self):
        meta = {"type": "DataBaseDatastore"}
        meta["db_config"] = self.db_config
        meta["query"] = self.query
        meta["mapping"] = self.mapping
        meta["base_dir"] = self.base_dir
        return meta
