"""Distinct types of annotations."""
from pony.orm import Required, Optional, Set, Json
from .base import Annotation


class Interval(Annotation):
    """Interval annotation."""

    start_time = Required(float)
    end_time = Required(float)


class Bbox(Interval):
    """Bounding box annotation."""

    max_freq = Required(float)
    min_freq = Required(float)
    wkt = Required(str)
    vertices = Required(Json)


class MultiLineString(Bbox):
    """Multilinestring annotation."""

    linestrings = Set(lambda: Linestring)


class Linestring(Bbox):
    """Linestring annotation."""

    multilinestring = Optional(MultiLineString)


class MultiPolygon(Bbox):
    """Multilinestring annotation."""

    polygons = Optional(lambda: Polygon)


class Polygon(Bbox):
    """Polygon annotation."""

    multipolygon = Optional(MultiPolygon)
