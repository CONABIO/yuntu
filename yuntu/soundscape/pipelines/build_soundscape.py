"""Soundscape base pipeline."""
from yuntu.core.audio.features.spectrogram import N_FFT
from yuntu.core.audio.features.spectrogram import HOP_LENGTH
from yuntu.core.audio.features.spectrogram import WINDOW_FUNCTION
from yuntu.core.pipeline.base import Pipeline
from yuntu.core.pipeline.places.extended import place

from yuntu.soundscape.processors.indices.direct import EXAG
from yuntu.soundscape.processors.indices.direct import INFORMATION
from yuntu.soundscape.processors.indices.direct import CORE
from yuntu.soundscape.processors.indices.direct import TOTAL

from yuntu.soundscape.transitions.basic_trans import as_dd, add_hash
from yuntu.soundscape.transitions.index_trans import slice_features

INDICES = [TOTAL(), EXAG(), INFORMATION(), CORE()]
TIME_UNIT = 60
FREQUENCY_BINS = 100
FREQUENCY_LIMITS = (0, 10000)
FEATURE_TYPE = 'spectrogram'
FEATURE_CONFIG = {"n_fft": N_FFT,
                  "hop_length": HOP_LENGTH,
                  "window_function": WINDOW_FUNCTION}
HASHER_CONFIG = {
    "module":{
        "object_name": "yuntu.soundscape.hashers.crono.CronoHasher"
    },
    "kwargs": {}
}
HASH_NAME = 'crono_hash'


class Soundscape(Pipeline):
    """Basic soundscape pipeline"""

    def __init__(self,
                 name,
                 recordings,
                 indices=INDICES,
                 time_unit=TIME_UNIT,
                 frequency_bins=FREQUENCY_BINS,
                 frequency_limits=FREQUENCY_LIMITS,
                 feature_type=FEATURE_TYPE,
                 feature_config=FEATURE_CONFIG,
                 **kwargs):
        super().__init__(name, **kwargs)
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
        slice_config_dict = {'time_unit': self.time_unit,
                             'frequency_bins': self.frequency_bins,
                             'frequency_limits': self.frequency_limits,
                             'feature_type': self.feature_type,
                             'feature_config': self.feature_config}
        self['slice_config'] = place(data=slice_config_dict,
                                     name='slice_config',
                                     ptype='dict')
        self['recordings'] = place(data=self.recordings,
                                   name='recordings',
                                   ptype='pandas_dataframe')
        self['indices'] = place(data=self.indices,
                                name='indices',
                                ptype='pickleable')
        self['npartitions'] = place(data=10,
                                    name='npartitions',
                                    ptype='scalar')
        self['recordings_dd'] = as_dd(self['recordings'],
                                      self['npartitions'])
        self['index_results'] = slice_features(self['recordings_dd'],
                                               self['slice_config'],
                                               self['indices'])



class HashedSoundscape(Soundscape):
    """Hashed soundscape pipeline.

    A hashed soundscape is a soundscape that has a special column 'hash'
    indicating some kind of aggregation criteria generated by a Hasher object.
    """

    def __init__(self,
                 *args,
                 hasher_config=HASHER_CONFIG,
                 hash_name=HASH_NAME,
                 **kwargs):
        if not isinstance(row_hasher, Hasher):
            raise ValueError("Argument 'hasher' must be of type Hasher.")
        self.hasher = row_hasher
        self.hash_name = hash_name
        super().__init__(*args, **kwargs)

    def build(self):
        """Build soundscape processing pipeline."""
        super().build()

        self['hasher_config'] = place(data=self.hasher_config,
                                      name='hasher',
                                      ptype='pickleable')
        self['hash_name'] = place(data=self.hash_name,
                                  name="hash_name",
                                  ptype='scalar')
        self['hashed_soundscape'] = add_hash(self['index_results'],
                                             self['hasher_config'],
                                             self['hash_name'])
