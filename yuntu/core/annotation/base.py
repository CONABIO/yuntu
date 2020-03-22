"""Annotation base classes.

These classes wrap interactions with annotation data to be used for audio
processing.
"""
from pony.orm.core import Entity
from yuntu.core.atlas.base import Chart
from yuntu.core.atlas.utils import plot_geometry, geom_from_wkt


class Annotation:
    """Basic class to manipulate attributes.

    Annotation class receives the same fields that are necessary for a
    database insert.
    """

    _mandatory_attr = ["recording",
                       "start_time",
                       "end_time",
                       "min_freq",
                       "max_freq",
                       "wkt",
                       "label",
                       "metadata"]

    def __init__(self, meta):
        """Initialize annotation."""
        self.id = None
        self.recording = None
        self.start_time = None
        self.end_time = None
        self.min_freq = None
        self.max_freq = None
        self.wkt = None
        self.label = None
        self.metadata = None
        self.db_entry = None
        self._geometry = None
        self._bbox = None
        self._chart = None
        if not isinstance(meta, dict):
            raise ValueError("Input should be of type 'dict'. Use " +
                             "'from_instance' or 'new' to create an " +
                             "annotation with a  database instance.")
        self.load_config(meta)

    def __repr__(self):
        """Repr annotation."""
        return f'Annotation: ({self.wkt})'

    def __str__(self):
        """Annotation as string."""
        return self.wkt

    def to_dict(self):
        """Annotation to dict."""
        meta = {}
        for key in self._mandatory_attr:
            meta[key] = getattr(self, key)
        if self.id is not None:
            meta["id"] = self.id
        return meta

    def load_config(self, config):
        """Load configuration.

        Parse configuration and load class attributes.

        Parameters
        ----------
            config: dict
                Dictionary with all parameters.

        Raises
        ------
            ValueError
                If any field is missing.
        """
        for key in self._mandatory_attr:
            if key not in config:
                raise ValueError("Field " + key +
                                 " is missing in configuration.")
            setattr(self, key, config[key])
        if "id" in config:
            self.id = config["id"]
        if "db_entry" in config:
            self.db_entry = config["db_entry"]

    @classmethod
    def from_instance(cls, instance):
        """Return annotation from database instance.

        Build self from database Annotation instance.

        Parameters
        ----------
            instance: pony.orm.core.Entity
                Database annotation instance.

        Returns
        -------
            annotation: Annotation
                Object of this class.

        Raises
        ------
            ValueError
                If type of instance is wrong or instances is not an entity
                instance.
        """
        meta = {}
        for key in cls._mandatory_attr:
            if not hasattr(instance, key):
                raise ValueError("Field " + key + " is missing" +
                                 "Incorrect entity type.")
            meta[key] = getattr(instance, key)
        meta["id"] = instance.id
        meta["db_entry"] = instance
        return cls(meta)

    @classmethod
    def from_dict(cls, meta):
        """Return annotation from configuration dictionary.

        Parse configuration and load class attributes.

        Parameters
        ----------
            meta: dict
                Dictionary with all parameters.

        Returns
        -------
            annotation: Annotation
                Object of this class.
        """
        return cls(meta)

    @classmethod
    def new(cls, meta):
        """Build from either an instance or a dictionary.

        Parse configuration and load class attributes.

        Parameters
        ----------
            meta: dict | pony.orm.core.Entity
                Dictionary with all parameters or database instance.

        Returns
        -------
            annotation: Annotation
                Object of this class.
        """
        if isinstance(meta, Entity):
            return cls.from_instance(meta)
        return cls(meta)

    @property
    def bbox(self):
        """Return annotation's bounding box.

        Return polygon representing time and frequency limits.

        Returns
        -------
        bbox: shapely.geometry.polygon.Polygon
        """
        if self._bbox is None:
            self._bbox = self.chart.bbox
        return self._bbox

    @property
    def geometry(self):
        """Return annotation's geometry.

        Return geometry associated to annotation.

        Returns
        -------
        geometry: shapely.geometry
        """
        if self._geometry is None:
            self._geometry = geom_from_wkt(self.wkt)
        return self._geometry

    @property
    def chart(self):
        """Return fragment corresponding to annotation.

        Produces an object of type Chart that holds information about
        annotation's boundaries.

        Returns
        -------
            chart: Chart
                Annotation's chart.
        """
        if self._chart is None:
            self._chart = Chart(self.start_time, self.end_time,
                                self.min_freq, self.max_freq)
        return self._chart

    def plot(self, ax=None, outpath=None, **kwargs):
        """Plot annotation geometry."""
        plot_geometry(self.geometry, ax, outpath, **kwargs)
