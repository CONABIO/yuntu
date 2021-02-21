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
        """Apply probe and return matches."""

    @abstractmethod
    def clean(self):
        """Remove memory footprint."""

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
    """A probe that use any kind of detection or multilabelling model."""

    def __init__(self, model_path):
        self._model = None
        self.model_path = model_path

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
