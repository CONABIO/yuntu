"""Base classes for reference systems.

An atlas is a collection of charts with a reference system over time and
frequency.
"""
from yuntu.core.atlas.utils import bbox_to_polygon, \
                                   plot_geometry, \
                                   reference_system


class Chart:
    """A delimited region of time and frequency."""

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
        return f'Chart: ({self.wkt})'

    def __str__(self):
        """Chart as string."""
        return self.wkt

    def to_dict(self):
        """Chart to dict"""
        return {"start_time": self.start_time,
                "end_time": self.end_time,
                "min_freq": self.min_freq,
                "max_freq": self.max_freq}

    def to_tuple(self):
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
            self._wkt = self.geometry.wkt
        return self._wkt

    def plot(self, ax=None, outpath=None, **kwargs):
        """Plot chart geometry."""
        plot_geometry(self.geometry, ax, outpath, **kwargs)


class Atlas:
    """A collection of compatible charts."""

    _atlas = {}
    range = None

    def __init__(self,
                 time_win,
                 time_hop,
                 freq_win,
                 freq_hop,
                 bounds):
        self.time_win = time_win
        self.time_hop = time_hop
        self.freq_win = freq_win
        self.freq_hop = freq_hop
        self.bounds = bounds
        self.build()

    def __iter__(self):
        """Iterate over charts within atlas."""
        for coords in self._atlas:
            yield self._atlas[coords], coords

    def build(self):
        """Build system of charts based on input parameters."""
        ref_system, self.range = reference_system(self.time_win, self.time_hop,
                                                  self.freq_win, self.freq_hop,
                                                  self.bounds)
        for coords in ref_system:
            self._atlas[coords] = Chart(*ref_system[coords])

    def chart(self, atlas_coords):
        """Return chart at specified atlas coordinates."""
        if atlas_coords in self._atlas:
            return self._atlas[atlas_coords]
        raise ValueError("Atlas coordinates out of range.")

    def intersects(self, geometry):
        """Return charts that intersect geometry."""
        return [(self._atlas[coords], coords)
                for coords in self._atlas
                if self._atlas[coords].geometry.intersects(geometry)]

    def within(self, geometry):
        """Return charts that lie within polygon."""
        return [(self._atlas[coords], coords)
                for coords in self._atlas
                if self._atlas[coords].geometry.within(geometry)]

    def contains(self, geometry):
        """Return charts that lie within polygon."""
        return [(self._atlas[coords], coords)
                for coords in self._atlas
                if self._atlas[coords].geometry.contains(geometry)]

    def plot(self, ax=None, outpath=None, **kwargs):
        """Plot atlas."""
        all_coords = list(self._atlas.keys())
        ncharts = len(all_coords)
        for i in range(ncharts - 1):
            plot_geometry(self._atlas[all_coords[i]].geometry, ax, **kwargs)
        plot_geometry(self._atlas[all_coords[ncharts - 1]].geometry,
                      ax=ax, outpath=outpath, **kwargs)
