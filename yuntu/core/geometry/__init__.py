from yuntu.core.geometry.base import Geometry
from yuntu.core.geometry.bbox import BBox
from yuntu.core.geometry.geometry_collections import GeometryCollection
from yuntu.core.geometry.intervals import TimeInterval
from yuntu.core.geometry.intervals import FrequencyInterval
from yuntu.core.geometry.lines import TimeLine
from yuntu.core.geometry.lines import FrequencyLine
from yuntu.core.geometry.linestrings import LineString
from yuntu.core.geometry.linestrings import MultiLineString
from yuntu.core.geometry.points import Point
from yuntu.core.geometry.points import MultiPoint
from yuntu.core.geometry.polygons import Polygon
from yuntu.core.geometry.polygons import MultiPolygon
from yuntu.core.geometry.weak import Weak


def geometry(geom):
    return Geometry.from_geometry(geom)


__all__ = [
    'geometry',
    'Geometry',
    'BBox',
    'GeometryCollection',
    'TimeInterval',
    'FrequencyInterval',
    'TimeLine',
    'FrequencyLine',
    'LineString',
    'Point',
    'MultiPoint',
    'Polygon',
    'MultiPolygon',
    'MultiLineString',
    'Weak',
]
