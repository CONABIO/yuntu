import tensorflow.keras as keras
from yuntu.soundscape.probes.base import ModelProbe

class KerasModelProbe(Probe, ABC):
    """A probe that use any kind of detection or multilabelling model."""

    def load_model(self):
        """Load model from model path."""
        self._model = keras.models.load_model(self.model_path, compile=False)

    def clean(self):
        """Remove memory footprint."""
        del self._model
        keras.backend.clear_session()
