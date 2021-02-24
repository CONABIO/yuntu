"""Classes for audio probes."""
from abc import ABC
from abc import abstractmethod

class Probe(ABC):
    """Base class for all probes.

    Given a signal, a probe is a method that tests matching
    criteria against it and returns a list of media slices that
    satisfy them.
    """

    @abstractmethod
    def apply(self, target, **kwargs):
        """Apply probe and output a list of dicts with a geometry attribute.

        The output should be a list of dictionaries with the following minimal
        structure and information:
        {
            'geometry': <yuntu.core.geometry.Geometry>,
            'tag': <str>,
            'score': <dict>
        }

        """

    @abstractmethod
    def clean(self):
        """Remove memory footprint."""

    def prepare_annotation(self, output):
        """Buid annotation from individual output"""

        geom = output["geometry"]
        start_time, min_freq, end_time, max_freq = geom.geometry.bounds
        wkt = geom.geometry.wkt
        meta = {"score": meta["score"]}
        geom_type = geom.name

        return {
            "labels": [{"key": "tag", "value": output["tag"], "type": "model_tag"}],
            "type": f"{geom_type}Annotation",
            "start_time": start_time,
            "end_time": end_time,
            "max_freq": max_freq,
            "min_freq": min_freq,
            "geometry": wkt,
            "metadata": meta
        }

    @property
    def info(self):
        return {
            'probe_class': self.__class__.__name__
        }

    def annotate(self,*args, **kwargs):
        """Apply probe and produce annotations"""

        outputs = self.apply(*args, **kwargs)
        annotations = []
        for o in outputs:
            ann_dict = self.prepare_annotation(o)
            if "metadata" not in ann_dict:
                ann_dict["metadata"] = {}
            ann_dict["metadata"]["probe_info"] = self.info
            annotations.append(ann_dict)
        return annotations

    def __enter__(self):
        """Behaviour for context manager"""
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Behaviour for context manager"""
        self.clean()

    def __call__(self, target, **kwargs):
        """Call apply method."""
        return self.apply(target, **kwargs)

class TemplateProbe(Probe, ABC):
    """A probe that uses a template to find similar matches."""

    @property
    @abstractmethod
    def template(self):
        """Return probe's template."""

    @abstractmethod
    def compare(self, target):
        """Compare target with self's template."""

class ModelProbe(Probe, ABC):
    """A probe that uses any kind of detection or multilabelling model"""

    def __init__(self, model_path):
        self._model = None
        self.model_path = model_path

    @property
    def info(self):
        return {
            'probe_class': self.__class__.__name__,
            'model_path': self.model_path
        }

    @property
    def model(self):
        if self._model is None:
            self.load_model()
        return self._model

    @abstractmethod
    def load_model(self):
        """Load model from model path."""

    @abstractmethod
    def predict(self, target):
        """Return self model's raw output."""
