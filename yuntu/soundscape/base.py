"""Soundscape base pipeline."""
from yuntu.core.pipeline.dask import Pipeline
from yuntu.core.pipeline.places.base import DictPlace
from yuntu.core.pipeline.places.base import ScalarPlace
from yuntu.core.pipeline.places.base import PickleablePlace
from yuntu.core.pipeline.places.extended import PandasDataFramePlace
import yuntu.soundscape.operations as ops
from yuntu.soundscape.indices import EXAG
from yuntu.soundscape.indices import INFORMATION
from yuntu.soundscape.indices import CORE
from yuntu.soundscape.indices import TOTAL
from yuntu.core.audio.features.spectrogram import N_FFT
from yuntu.core.audio.features.spectrogram import HOP_LENGTH
from yuntu.core.audio.features.spectrogram import WINDOW_FUNCTION


INDICES = [TOTAL(), EXAG(), INFORMATION(), CORE()]
TIME_UNIT = 60
FREQUENCY_BINS = 100
FREQUENCY_LIMITS = (0, 10000)
FEATURE_TYPE = 'spectrogram'
FEATURE_CONFIG = {"n_fft": N_FFT,
                  "hop_length": HOP_LENGTH,
                  "window_function": WINDOW_FUNCTION}


class SoundscapePipeline(Pipeline):
    """Base class for soundscape pipelines."""

    def __init__(self,
                 *args,
                 recordings,
                 indices=INDICES,
                 time_unit=TIME_UNIT,
                 frequency_bins=FREQUENCY_BINS,
                 frequency_limits=FREQUENCY_LIMITS,
                 feature_type=FEATURE_TYPE,
                 feature_config=FEATURE_CONFIG,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if not isinstance(indices, (tuple, list)):
            message = "Argument 'indices' must be a tuple or a list of " + \
                      " acoustic indices."
            raise ValueError(message)
        self.recordings = recordings
        self.indices = indices
        self.time_unit = time_unit
        self.frequency_bins = frequency_bins
        self.frequency_limits = frequency_limits
        self.feature_type = feature_type
        self.feature_config = feature_config
        self.build()

    def build(self):
        """Build soundscape processing pipeline."""
        slice_config = (DictPlace(name='slice_config',
                        data={'time_unit': self.time_unit,
                              'frequency_bins': self.frequency_bins,
                              'frequency_limits': self.frequency_limits,
                              'feature_type': self.feature_type,
                              'feature_config': self.feature_config},
                        pipeline=self))
        recordings = (PandasDataFramePlace(name='recordings',
                      data=self.recordings,
                      pipeline=self))
        indices = (PickleablePlace(name='indices',
                   data=self.indices,
                   pipeline=self))
        npartitions = ScalarPlace("npartitions",
                                  data=10,
                                  pipeline=self)
        recordings_dd = ops.as_dd(recordings, npartitions)
        slice_features = ops.slice_features(recordings_dd,
                                            slice_config)
        index_results = ops.apply_indices(slice_features,
                                          indices)
