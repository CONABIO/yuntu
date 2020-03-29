"""Operations for soundscape Pipeline."""
import dask.dataframe as dd
from yuntu.core.audio.audio import Audio, MEDIA_INFO_FIELDS
from yuntu.core.pipeline.nodes.decorators import dd_op
from yuntu.soundscape.utils import slice_windows
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
                 read_samplerate=read_samplerate,
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


@dd_op(name='slice_features', is_output=False, persist=True)
def slice_features(recordings, config, meta):
    """Produce feature slices dataframe."""
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


@dd_op(name='apply_indices', is_output=True, persist=True)
def apply_indices(slices, indices, meta):
    """Apply acoustic indices to slices."""
    index_names = [index.name for index in indices]
    if len(index_names) != len(set(index_names)):
        message = "Index names have duplicates. Please use a diferent name" + \
                  " for each index to compute."
        raise ValueError(message)
    results = slices.apply(feature_indices,
                           meta=meta,
                           axis=1,
                           indices=indices)
    return results.drop(['feature_cut'], axis=1)


@dd_op(name='as_dd')
def as_dd(pd_dataframe, dask_config):
    """Transform audio dataframe to a dask dataframe for computations."""
    return dd.from_pandas(pd_dataframe,
                          npartitions=dask_config['npartitions'],
                          name="as_dd")
