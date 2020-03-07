"""Base classes for collection."""
from abc import ABC
from pony.orm import db_session
from yuntu.core.database.base import Recording, Annotation, YuntuDb
from yuntu.core.audio.base import Audio


class Collection(ABC):
    """Base class for all collections."""

    db_provider = 'sqlite'
    db_config = None

    def __init__(self, db_config=None):
        """Initialize collection."""
        self.db_config = db_config
        self.init_db()

    def init_db(self):
        """Bind database to provider."""
        YuntuDb.bind(self.db_provider, **self.db_config)

    @db_session
    def insert(self, meta_arr):
        """Directly insert new media entries without a datastore."""
        return [Recording(**meta) for meta in meta_arr]

    @db_session
    def annotate(self, meta_arr):
        """Insert annotations to database."""
        return [Annotation(**meta) for meta in meta_arr]

    @db_session
    def update(self, query, set_obj):
        """Update matches."""
        return [rec.set(**set_obj) for rec in Recording.select(query)]

    @db_session
    def delete(self, query):
        """Delete matches."""
        return [rec.delete for rec in Recording.select(query)]

    def transform(self, query, parser, mode):
        """Transform matches by parser."""

    def dump(self, dir_path):
        """Dump collection to 'dir_path'."""

    def load(self, dir_path):
        """Load collection from 'dir_path'."""

    def materialize(self, dir_path):
        """Persist collection in 'dir_path' including recordings."""

    @db_session
    def annotations(self, query, iterate=True):
        """Retrieve annotations from database."""
        if iterate:
            def iterator(query):
                for meta in Annotation.select(query):
                    yield meta
            return iterator
        return Annotation.select(query)

    @db_session
    def media(self, query, iterate=True):
        """Retrieve audio objects."""
        if iterate:
            def iterator(query):
                for meta in Recording.select(query):
                    yield Audio(meta)
            return iterator
        return [Audio(rec) for rec in Recording.select(query)]

    def pull(self, datastore):
        """Pull data from datastore and inserto to collection."""
