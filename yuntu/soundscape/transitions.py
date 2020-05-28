"""Operations for soundscape Pipeline."""
import numpy as np
import dask.dataframe as dd
from yuntu.core.audio.audio import Audio, MEDIA_INFO_FIELDS
from yuntu.core.pipeline.transitions.decorators import transition
from yuntu.soundscape.utils import slice_windows
from yuntu.core.pipeline.places import DictPlace
from yuntu.core.pipeline.places import PickleablePlace
from yuntu.core.pipeline.places import ScalarPlace
from yuntu.core.pipeline.places.extended import PandasDataFramePlace
from yuntu.core.pipeline.places.extended import DaskDataFramePlace
from yuntu.soundscape.hashers import Hasher
from yuntu.soundscape.dataframe import SoundscapeAccessor


def to_audio(row, read_samplerate=None, lazy=True):
    """Return Audio object using row values from an audio dataframe."""
    media_info = {}
    for field in MEDIA_INFO_FIELDS:
        media_info[field] = row[field]
    return Audio(path=row['path'],
                 timeexp=row['timeexp'],
                 media_info=media_info,
                 metadata=row['metadata'],
                 id=row['id'],
                 lazy=lazy)


def feature_slices(row, config):
    """Produce slices from recording and configuration."""
    audio = to_audio(row)
    cuts, weights = slice_windows(config["time_unit"],
                                  audio.media_info.duration,
                                  config["frequency_bins"],
                                  config["frequency_limits"])
    features = audio.features
    feature = getattr(features,
                      config["feature_type"])(**config["feature_config"])
    feature_cuts = [feature.cut(cut).array for cut in cuts]
    start_times = [cut.start for cut in cuts]
    end_times = [cut.end for cut in cuts]
    max_freqs = [cut.max for cut in cuts]
    min_freqs = [cut.min for cut in cuts]
    row['start_time'] = start_times
    row['end_time'] = end_times
    row['min_freq'] = max_freqs
    row['max_freq'] = min_freqs
    row['weight'] = weights
    row['feature_cut'] = feature_cuts
    return row


def feature_indices(row, indices):
    """Compute acoustic indices for one row."""
    for index in indices:
        row[index.name] = index(row['feature_cut'])
    return row


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

    meta = [(name, dtype)
            for name, dtype in zip(dataframe.columns,
                                   dataframe.dtypes.values)]

    meta.append((out_name, hasher.dtype))
    return dataframe.apply(hasher, out_name=out_name, meta=meta, axis=1)


@transition(name='slice_features', outputs=["feature_slices"], persist=True,
            signature=((DaskDataFramePlace, DictPlace), (DaskDataFramePlace,)))
def slice_features(recordings, config):
    """Produce feature slices dataframe."""
    meta = [(name, dtype)
            for name, dtype in zip(recordings.columns,
                                   recordings.dtypes.values)]
    meta += [('start_time', np.dtype('float64')),
             ('end_time', np.dtype('float64')),
             ('min_freq', np.dtype('float64')),
             ('max_freq', np.dtype('float64')),
             ('weight', np.dtype('float64')),
             ('feature_cut', np.dtype('float64'))]
    result = recordings.apply(feature_slices,
                              meta=meta,
                              axis=1,
                              config=config)
    exploded_slices = result[['id', 'start_time']].explode('start_time')
    exploded_slices['end_time'] = result['end_time'].explode()
    exploded_slices['min_freq'] = result['max_freq'].explode()
    exploded_slices['max_freq'] = result['min_freq'].explode()
    exploded_slices['weight'] = result['weight'].explode()
    exploded_slices['feature_cut'] = result['feature_cut'].explode()
    return exploded_slices


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

    meta = [('id', np.dtype('int64')),
            ('start_time', np.dtype('float64')),
            ('end_time', np.dtype('float64')),
            ('min_freq', np.dtype('float64')),
            ('max_freq', np.dtype('float64')),
            ('weight', np.dtype('float64')),
            ('feature_cut', np.dtype('float64'))]

    meta += [(index.name,
             np.dtype('float64'))
             for index in indices]

    results = slices.apply(feature_indices,
                           meta=meta,
                           axis=1,
                           indices=indices)

    return results.drop(['feature_cut'], axis=1)


@transition(name='as_dd', outputs=["recordings_dd"],
            signature=((PandasDataFramePlace, ScalarPlace),
                       (DaskDataFramePlace,)))
def as_dd(pd_dataframe, npartitions):
    """Transform audio dataframe to a dask dataframe for computations."""
    dask_dataframe = dd.from_pandas(pd_dataframe,
                                    npartitions=npartitions,
                                    name="as_dd")
    return dask_dataframe
