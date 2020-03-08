"""Base classes for collection."""
from abc import ABC
from yuntu.core.database.base import DatabaseManager
from yuntu.core.audio.base import Audio


class Collection(ABC):
    """Base class for all collections."""

    db_config = {
        'provider': 'sqlite',
        'configs': {
            'filename': ':memory:',
            'create_db': True
        }
    }
    audio_class = Audio
    db_manager_class = DatabaseManager

    def __init__(self, db_config=None):
        """Initialize collection."""
        if db_config is not None:
            self.db_config = db_config

        self.db_manager = self.get_db_manager()

    def get_db_manager(self):
        return self.db_manager_class(**self.db_config)

    def insert(self, meta_arr):
        """Directly insert new media entries without a datastore."""
        return self.db_manager.insert(meta_arr)

    def annotate(self, meta_arr):
        """Insert annotations to database."""
        return self.db_manager.insert(meta_arr, model="annotation")

    def update_recordings(self, query, set_obj):
        """Update matches."""
        return self.db_manager.update(query, set_obj, model="recordings")

    def update_annotations(self, query, set_obj):
        """Update matches."""
        return self.db_manager.update(query, set_obj, model="annotations")

    def delete_recordings(self, query):
        """Delete matches."""
        return self.db_manager.delete(query, model='recording')

    def delete_annotations(self, query):
        """Delete matches."""
        return self.db_manager.delete(query, model='annotation')

    @property
    def recordings_model(self):
        return self.db_manager.models.recording

    @property
    def annotations_model(self):
        return self.db_manager.models.annotation

    def annotations(self, query=None, iterate=True):
        """Retrieve annotations from database."""
        matches = self.db_manager.select(query, model="annotation")
        if iterate:
            return matches
        return list(matches)

    def get_audio_class(self):
        return self.audio_class

    def build_audio(self, recording):
        audio_class = self.get_audio_class()
        return audio_class(recording)

    def recordings(self, query=None, iterate=True):
        """Retrieve audio objects."""
        matches = self.db_manager.select(query, model="recording")
        if iterate:
            return matches
        return list(matches)

    def __iter__(self):
        for recording in self.recordings():
            yield self.build_audio(recording)

    def media(self, query=None, iterate=True):
        """Retrieve audio objects."""
        matches = self.db_manager.select(query, model="recording")
        if iterate:
            def iterator():
                for meta in matches:
                    yield self.build_audio(meta)
            return iterator
        return [self.build_audio(meta) for meta in matches]

    def pull(self, datastore):
        """Pull data from datastore and insert into collection."""

    def transform(self, query, parser, mode):
        """Transform matches by parser."""

    def dump(self, dir_path):
        """Dump collection to 'dir_path'."""

    def load(self, dir_path):
        """Load collection from 'dir_path'."""

    def materialize(self, dir_path):
        """Persist collection in 'dir_path' including recordings."""
