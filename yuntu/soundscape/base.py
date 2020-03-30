"""Soundscape base pipeline."""
import numpy as np
from yuntu.core.pipeline.dask import DaskPipeline, DASK_CONFIG
import yuntu.core.pipeline.nodes.inputs as pline_inputs
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
        self.build()

    def build(self):
        """Build soundscape processing pipeline."""
        dask_config = pline_inputs.DictInput("dask_config",
                                             data=DASK_CONFIG,
                                             pipeline=self)
        slice_config = (pline_inputs.DictInput(name='slice_config',
                        data={'time_unit': self.time_unit,
                              'frequency_bins': self.frequency_bins,
                              'frequency_limits': self.frequency_limits,
                              'feature_type': self.feature_type,
                              'feature_config': self.feature_config},
                        pipeline=self))

        meta_slices = [(name, dtype)
                       for name, dtype in zip(self.recordings.columns,
                                              self.recordings.dtypes.values)]
        meta_slices += [('start_time', np.dtype('float64')),
                        ('end_time', np.dtype('float64')),
                        ('min_freq', np.dtype('float64')),
                        ('max_freq', np.dtype('float64')),
                        ('weight', np.dtype('float64')),
                        ('feature_cut', np.dtype('float64'))]
        slices_meta = (pline_inputs.PickleableInput(name='slices_meta',
                       data=meta_slices,
                       pipeline=self))

        meta_indices = [('id', np.dtype('int64')),
                        ('start_time', np.dtype('float64')),
                        ('end_time', np.dtype('float64')),
                        ('min_freq', np.dtype('float64')),
                        ('max_freq', np.dtype('float64')),
                        ('weight', np.dtype('float64')),
                        ('feature_cut', np.dtype('float64'))]
        meta_indices += [(index.name,
                         np.dtype('float64'))
                         for index in self.indices]
        indices_meta = (pline_inputs.PickleableInput(name='indices_meta',
                        data=meta_indices,
                        pipeline=self))

        recordings = (pline_inputs.PandasDataFrameInput(name='recordings',
                      data=self.recordings,
                      pipeline=self))
        indices = (pline_inputs.PickleableInput(name='indices',
                   data=self.indices,
                   pipeline=self))
        recordings_dd = ops.as_dd(recordings, dask_config)
        slice_features = ops.slice_features(recordings_dd,
                                            slice_config,
                                            slices_meta)
        index_results = ops.apply_indices(slice_features,
                                          indices,
                                          indices_meta)

    def set_indices(self, indices):
        """Set soundscape indices."""
        self.indices = indices
        self.clear()

    def set_time_unit(self, time_unit):
        """Set time unit."""
        self.time_unit = time_unit
        self.clear()

    def set_frequency_bins(self, frequency_bins):
        """Set frequency limits."""
        self.frequency_bins = frequency_bins
        self.clear()

    def set_frequency_limits(self, frequency_limits):
        """Set frequency limits."""
        self.frequency_limits = frequency_limits
        self.clear()

    def set_feature_type(self, feature_type):
        """Set feature type."""
        self.feature_type = feature_type
        self.clear()

    def set_feature_config(self, feature_config):
        """Set feature configuration."""
        self.feature_config = feature_config
        self.clear()
