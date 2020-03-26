"""Soundscape base pipeline."""
import numpy as np
from yuntu.core.pipeline.dask import DaskPipeline
import yuntu.core.pipeline.soundscape.operations as ops
from yuntu.core.pipeline.soundscape.indices import EXAG, INFORMATION, CORE
from yuntu.core.audio.features.spectrogram import N_FFT
from yuntu.core.audio.features.spectrogram import HOP_LENGTH
from yuntu.core.audio.features.spectrogram import WINDOW_FUNCTION


INDICES = [EXAG(), INFORMATION(), CORE()]
TIME_UNIT = 60
FREQUENCY_BINS = 100
FREQUENCY_LIMITS = (0, 10000)
FEATURE_TYPE = 'spectrogram'
FEATURE_CONFIG = {"n_fft": N_FFT,
                  "hop_length": HOP_LENGTH,
                  "window_function": WINDOW_FUNCTION}


class SoundscapePipeline(DaskPipeline):
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
        self.build_pipeline()

    def build_pipeline(self):
        """Build soundscape processing pipeline."""
        slice_config = {'time_unit': self.time_unit,
                        'frequency_bins': self.frequency_bins,
                        'frequency_limits': self.frequency_limits,
                        'feature_type': self.feature_type,
                        'feature_config': self.feature_config}
        slices_meta = [('id', np.dtype('int64')),
                       ('window', np.dtype('O')),
                       ('weight', np.dtype('float64')),
                       ('feature_cut', np.dtype('O'))]
        indices_meta = [(index.name,
                         np.dtype('float64'))
                        for index in self.indices]
        self.add_input(name='recordings', data=self.recordings)
        self.add_input(name='indices', data=self.indices)
        self.add_input(name='slice_config', data=slice_config)
        self.add_input(name='slices_meta', data=slices_meta)
        self.add_input(name='indices_meta', data=indices_meta)
        self.add_operation(name='recordings_dd',
                           operation=ops.as_dask_dataframe,
                           inputs=('recordings'))
        self.add_operation(name='slice_features',
                           operation=ops.slice_features,
                           inputs=('recordings_dd',
                                   'slice_config',
                                   'slices_meta'),
                           is_output=False,
                           persist=True,
                           linearize=True)
        self.add_operation(name='apply_indices',
                           operation=ops.apply_indices,
                           inputs=('slices',
                                   'indices',
                                   'indices_meta'),
                           is_output=True,
                           persist=True,
                           linearize=True)
        self.linearize_operations()

    def rebuild(self):
        """Cear all data and rebuild."""
        self.clear()
        self.graph = {}
        self.build_pipeline()

    def set_indices(self, indices):
        """Set soundscape indices."""
        self.indices = indices
        self.rebuild()

    def set_time_unit(self, time_unit):
        """Set time unit."""
        self.time_unit = time_unit
        self.rebuild()

    def set_frequency_bins(self, frequency_bins):
        """Set frequency limits."""
        self.frequency_bins = frequency_bins
        self.rebuild()

    def set_frequency_limits(self, frequency_limits):
        """Set frequency limits."""
        self.frequency_limits = frequency_limits
        self.rebuild()

    def set_feature_type(self, feature_type):
        """Set feature type."""
        self.feature_type = feature_type
        self.rebuild()

    def set_feature_config(self, feature_config):
        """Set feature configuration."""
        self.feature_config = feature_config
        self.rebuild()
