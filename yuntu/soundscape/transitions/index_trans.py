"""Transitions for acoustic indices."""
import numpy as np
import pandas as pd

from yuntu.core.pipeline.transitions.decorators import transition
from yuntu.core.pipeline.places import PickleablePlace
from yuntu.core.pipeline.places.extended import DaskDataFramePlace
from yuntu.core.pipeline.places import *
from yuntu.soundscape.dataframe import SoundscapeAccessor


def feature_indices(row, indices):
    """Compute acoustic indices for one row."""
    new_row = {}
    for index in indices:
        new_row[index.name] = index(row['feature_cut'])
    return pd.Series(new_row)


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

@transition(name='apply_indices', outputs=["index_results"],
            is_output=True, persist=True, keep=True,
            signature=((DaskDataFramePlace, PickleablePlace),
                       (DaskDataFramePlace, )))
def apply_indices(slices, indices):
    """Apply acoustic indices to slices."""
    index_names = [index.name for index in indices]
    if len(index_names) != len(set(index_names)):
        message = "Index names have duplicates. Please use a diferent name" + \
                  " for each index to compute."
        raise ValueError(message)

    meta = [(index.name,
            np.dtype('float64'))
            for index in indices]

    results = slices.apply(feature_indices,
                           meta=meta,
                           axis=1,
                           indices=indices)
    for index in indices:
        slices[index.name] = results[index.name]

    return slices.drop(['feature_cut'], axis=1)
