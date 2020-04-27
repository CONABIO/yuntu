"""Base classes for collection."""
import pandas as pd

from yuntu.core.database.base import DatabaseManager
from yuntu.core.database.base import TimedDatabaseManager
from yuntu.core.audio.audio import Audio
from yuntu.core.annotation.annotation import Annotation


class Collection:
    """Base class for all collections."""

    db_config = {
        'provider': 'sqlite',
        'config': {
            'filename': ':memory:',
            'create_db': True
        }
    }
    audio_class = Audio
    annotation_class = Annotation
    db_manager_class = DatabaseManager

    def __init__(self, db_config=None):
        """Initialize collection."""
        if db_config is not None:
            self.db_config = db_config

        self.db_manager = self.get_db_manager()

    def __getitem__(self, key):
        queryset = self.recordings()
        if isinstance(key, int):
            return self.build_audio(queryset[key:key + 1][0])

        return [self.build_audio(recording) for recording in queryset[key]]

    def __iter__(self):
        for recording in self.recordings():
            yield self.build_audio(recording)

    def __len__(self):
        return len(self.recordings())

    def get(self, key):
        record = self.recordings(lambda rec: rec.id == key).get()
        return self.build_audio(record)

    def get_recording_dataframe(self, query=None, limit=None, offset=None):
        if query is not None:
            recordings = self.db_manager.select(query, model="recording")
        else:
            recordings = self.recordings()
        records = []
        if limit is not None or offset is not None:
            has_offset = False
            has_limit = False
            if offset is not None:
                if not isinstance(offset, int):
                    message = "Offset must be an integer."
                    raise ValueError(message)
                has_offset = True
            if limit is not None:
                if not isinstance(limit, int):
                    message = "Limit must be an integer."
                    raise ValueError(message)
                has_limit = True
            if has_offset and has_limit:
                recordings = recordings[offset:offset+limit]
            elif has_offset:
                recordings = recordings[offset:]
            elif has_limit:
                recordings = recordings[:limit]
        for recording in recordings:
            data = recording.to_dict()
            media_info = data.pop('media_info')
            data.update(media_info)
            records.append(data)

        return pd.DataFrame(records)

    def get_annotation_dataframe(self, query=None):
        if query is not None:
            annotations = self.db_manager.select(query, model="annotation")
        else:
            annotations = self.annotations()
        records = []
        for annotation in annotations:
            data = annotation.to_dict()
            labels = data.pop('labels')

            for label in labels:
                data[label['key']] = label['value']

            records.append(data)

        return pd.DataFrame(records)

    def get_db_manager(self):
        return self.db_manager_class(**self.db_config)

    def insert(self, meta_arr):
        """Directly insert new media entries without a datastore."""
        if not isinstance(meta_arr, (list, tuple)):
            meta_arr = [meta_arr]
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

    def recordings(self, query=None, iterate=True):
        """Retrieve audio objects."""
        matches = self.db_manager.select(query, model="recording")
        if iterate:
            return matches
        return list(matches)

    def build_audio(self, recording):
        annotations = []
        for annotation in recording.annotations:
            data = annotation.to_dict()
            annotation = self.annotation_class.from_record(data)
            annotations.append(annotation)

        return self.audio_class(
            path=recording.path,
            id=recording.id,
            media_info=recording.media_info,
            timeexp=recording.timeexp,
            metadata=recording.metadata,
            annotations=annotations,
            lazy=True)

    def pull(self, datastore):
        """Pull data from datastore and insert into collection."""
        datastore.insert_into(self)

    def dump(self, dir_path):
        """Dump collection to 'dir_path'."""

    def load(self, dir_path):
        """Load collection from 'dir_path'."""

    def materialize(self, dir_path):
        """Persist collection in 'dir_path' including recordings."""


class TimedCollection(Collection):
    """Time aware collection."""
    db_manager_class = TimedDatabaseManager
