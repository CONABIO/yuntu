"""Distinct types of annotations."""
from pony.orm import Required
from pony.orm import Optional
from pony.orm import PrimaryKey
from pony.orm import Json


WEAK_ANNOTATION = 'weak'
INTERVAL_ANNOTATION = 'interval'
BBOX_ANNOTATION = 'bbox'
LINESTRING_ANNOTATION = 'linestring'

ANNOTATION_TYPES = [
    WEAK_ANNOTATION,
    INTERVAL_ANNOTATION,
    BBOX_ANNOTATION,
    LINESTRING_ANNOTATION,
]


def build_base_annotation_model(db):
    """Create base annotation model."""
    class Annotation(db.Entity):
        """Basic annotation entity for yuntu."""

        id = PrimaryKey(int, auto=True)
        recording = Required('Recording')

        notetype = Required(str)
        label = Required(Json)
        metadata = Required(Json)

        start_time = Optional(float)
        end_time = Optional(float)
        max_freq = Optional(float)
        min_freq = Optional(float)

        wkt = Optional(str)
        vertices = Optional(Json)

        def before_insert(self):
            if self.notetype not in ANNOTATION_TYPES:
                message = f'Notetype {self.notetype} not implemented'
                raise NotImplementedError(message)

            if self.notetype == WEAK_ANNOTATION:
                return

            if self.start_time is None or self.end_time is None:
                message = (
                    f'Annotation type {self.notetype} requires setting '
                    'a starting and ending time (start_time and end_time)')
                raise ValueError(message)

            if self.notetype == INTERVAL_ANNOTATION:
                return

            if self.max_freq is None or self.min_freq is None:
                message = (
                    f'Annotation type {self.notetype} requires setting '
                    'a maximum and minimum frequency (max_freq and min_freq)')
                raise ValueError(message)

            if self.notetype == BBOX_ANNOTATION:
                return

            if self.wkt is None or self.verts is None:
                message = (
                    f'Annotation type {self.notetype} requires setting '
                    'a wkt string and vertices array (wkt and vertices)')
                raise ValueError(message)
    return Annotation
