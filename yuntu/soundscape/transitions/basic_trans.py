
"""Transitions for basic usage."""
import numpy as np
import pandas as pd
import dask.dataframe as dd
from yuntu.soundscape.utils import slice_windows
from yuntu.core.pipeline.transitions.decorators import transition
from yuntu.core.audio.audio import Audio, MEDIA_INFO_FIELDS
from yuntu.core.pipeline.places import DictPlace
from yuntu.core.pipeline.places import ScalarPlace
from yuntu.core.pipeline.places import PickleablePlace
from yuntu.core.pipeline.places.extended import PandasDataFramePlace
from yuntu.core.pipeline.places.extended import DaskDataFramePlace
from yuntu.soundscape.hashers.base import Hasher
from yuntu.soundscape.dataframe import SoundscapeAccessor


def feature_slices(row, audio, config, indices):
    """Produce slices from recording and configuration."""
    cuts, weights = slice_windows(config["time_unit"],
                                  audio.duration,
                                  config["frequency_bins"],
                                  config["frequency_limits"])
    feature = getattr(audio.features,
                      config["feature_type"])(**config["feature_config"])
    audio.clean()
    feature_cuts = [feature.cut_array(cut) for cut in cuts]
    feature.clean()

    start_times = [cut.start for cut in cuts]
    end_times = [cut.end for cut in cuts]
    max_freqs = [cut.max for cut in cuts]
    min_freqs = [cut.min for cut in cuts]

    new_row = {}
    new_row['start_time'] = start_times
    new_row['end_time'] = end_times
    new_row['min_freq'] = max_freqs
    new_row['max_freq'] = min_freqs
    new_row['weight'] = weights

    for index in indices:
        results = []
        for fcut in feature_cuts:
            results.append(index(fcut))
        new_row[index.name] = results

    return pd.Series(new_row)

@transition(name='add_hash', outputs=["hashed_soundscape"],
            keep=True, persist=True, is_output=True,
            signature=((DaskDataFramePlace, PickleablePlace, ScalarPlace),
                       (DaskDataFramePlace, )))
def add_hash(dataframe, hasher, out_name="xhash"):
    if not isinstance(hasher, Hasher):
        raise ValueError("Argument 'hasher' must be of class Hasher.")
    if not hasher.validate(dataframe):
        str_cols = str(hasher.columns)
        message = ("Input dataframe is incompatible with hasher."
                   f"Missing column inputs. Hasher needs: {str_cols} ")
        raise ValueError(message)

    meta = [(out_name, hasher.dtype)]
    result = dataframe.apply(hasher, out_name=out_name, meta=meta, axis=1)
    dataframe[out_name] = result[out_name]

    return dataframe


@transition(name='slice_features', outputs=["feature_slices"], persist=True,
            signature=((DaskDataFramePlace, DictPlace, PickleablePlace),
                       (DaskDataFramePlace,)))
def slice_features(recordings, config, indices):
    """Produce feature slices dataframe."""

    meta = [('start_time', np.dtype('float64')),
            ('end_time', np.dtype('float64')),
            ('min_freq', np.dtype('float64')),
            ('max_freq', np.dtype('float64')),
            ('weight', np.dtype('float64'))]

    meta += [(index.name,
             np.dtype('float64'))
             for index in indices]

    result = recordings.audio.apply(feature_slices,
                                    meta=meta,
                                    config=config,
                                    indices=indices)

    recordings['start_time'] = result['start_time']

    slices = recordings.explode('start_time')
    slices['end_time'] = result['end_time'].explode()
    slices['min_freq'] = result['max_freq'].explode()
    slices['max_freq'] = result['min_freq'].explode()
    slices['weight'] = result['weight'].explode()

    for index in indices:
        slices[index.name] = result[index.name].explode()

    return slices


@transition(name='as_dd', outputs=["recordings_dd"],
            signature=((PandasDataFramePlace, ScalarPlace),
                       (DaskDataFramePlace,)))
def as_dd(pd_dataframe, npartitions):
    """Transform audio dataframe to a dask dataframe for computations."""
    dask_dataframe = dd.from_pandas(pd_dataframe,
                                    npartitions=npartitions,
                                    name="as_dd")
    return dask_dataframe