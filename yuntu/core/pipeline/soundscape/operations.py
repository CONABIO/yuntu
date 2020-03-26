"""Operations for soundscape Pipeline."""
import dask.dataframe as dd
from yuntu.core.pipeline.soundscape.utils import slice_windows


def feature_slices(row, config):
    """Produce slices from recording and configuration."""
    audio = row['audio']
    cuts, weights = slice_windows(config["time_unit"],
                                  audio.duration,
                                  config["frequency_bins"],
                                  config["frequency_limits"])
    feature = audio.features[config['feature_type']](config["feature_config"])
    feature_cuts = [feature.cut(cut) for cut in cuts]
    return cuts, weights, feature_cuts


def feature_indices(row, indices):
    """Compute acoustic indices for one row."""
    results = [index(row['feature_cut'].array) for index in indices]
    return results


def slice_features(recordings, config, meta):
    """Produce feature slices dataframe."""
    slices = recordings[["id"]]
    slices[['window',
            'weight',
            'feature_cut']] = recordings.apply(feature_slices,
                                               meta=meta,
                                               axis=1,
                                               config=config)
    return slices.explode(('window', 'weight', 'feature_cut'))


def apply_indices(slices, indices, indices_meta):
    """Apply acoustic indices to slices."""
    index_names = [imeta[0] for imeta in indices_meta]
    if len(index_names) != len(set(index_names)):
        message = "Index names have duplicates. Please use a diferent name" + \
                  " for each index to compute."
        raise ValueError(message)

    results = slices[['id', 'window', 'weight']]
    results[index_names] = slices.apply(feature_indices,
                                        meta=indices_meta,
                                        axis=1,
                                        indices=indices)
    return results


def as_dask_dataframe(pd_dataframe, npartitions):
    """Transform audio dataframe to a dask dataframe for computations."""
    return dd.from_pandas(pd_dataframe, npartitions=npartitions)
