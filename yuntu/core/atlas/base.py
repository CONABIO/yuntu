"""Base classes for reference systems.

An atlas is a collection of charts with a reference system over time and
frequency.
"""
from yuntu.core.atlas.utils import bbox_to_polygon, plot_geometry


class Chart:
    """A delimited region of time and frequency."""

    start_time = None
    end_time = None
    min_freq = None
    max_freq = None
    _bbox = None
    _wkt = None
    _geometry = None

    def __init__(self, start_time, end_time, min_freq, max_freq):
        """Initialize chart."""
        self.start_time = start_time
        self.end_time = end_time
        self.min_freq = min_freq
        self.max_freq = max_freq

    def __repr__(self):
        """Repr chart."""
        return f'Chart: ({str(self.bbox)})'

    def __str__(self):
        """Chart to string."""
        return self.wkt

    def __dict__(self):
        """Chart to dict"""
        return {"start_time": self.start_time,
                "end_time": self.end_time,
                "min_freq": self.min_freq,
                "max_freq": self.max_freq}

    def __tuple__(self):
        """Chart to tuple."""
        return tuple(self.bbox)

    @property
    def bbox(self):
        """Return bounding box of chart."""
        if self._bbox is None:
            self._bbox = (self.start_time, self.end_time,
                          self.min_freq, self.max_freq)
        return self._bbox

    @property
    def geometry(self):
        """Return chart as shapely Polygon."""
        if self._geometry is None:
            self._geometry = bbox_to_polygon(self.bbox)
        return self._geometry

    @property
    def wkt(self):
        """Return chart geometry as wkt."""
        if self._wkt is None:
            self._wkt = self.geometry.ExportToWkt()
        return self._wkt

    def plot(self, ax=None, **kwargs):
        """Plot chart geometry."""
        plot_geometry(self.geometry, ax, **kwargs)
