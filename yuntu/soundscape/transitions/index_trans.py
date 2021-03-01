"""Transitions for acoustic indices."""
import numpy as np
import pandas as pd
import datetime

from yuntu.core.pipeline.transitions.decorators import transition
from yuntu.core.pipeline.places import PickleablePlace
from yuntu.core.pipeline.places.extended import DaskDataFramePlace
from yuntu.core.pipeline.places import *
from yuntu.soundscape.dataframe import SoundscapeAccessor
from yuntu.soundscape.utils import slice_windows, sliding_slice_windows, aware_time

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

def write_timed_grid_slices(row, audio, slice_config, write_config):
    """Produce slices from recording and configuration."""
    cuts = sliding_slice_windows(audio.duration,
                                 config["time_unit"],
                                 config["time_hop"],
                                 config["frequency_limits"],
                                 config["frequency_unit"],
                                 config["frequency_hop"])
    feature = getattr(audio.features,
                      config["feature_type"])(**config["feature_config"])
    audio.clean()
    feature_cuts = [feature.cut_array(cut) for cut in cuts]
    feature.clean()

    strtime = row["time_raw"]
    timezone = row["time_zone"]
    timeformat = row["time_format"]

    atime = aware_time(strtime, timezone, timeformat)

    include_meta = {}
    if "include_meta" in slice_config:
        include_meta = {key: np.array([str(row["metadata"][key])])
                        for key in slice_config["include_meta"]}

    write_results = []
    recording_id = row["id"]
    recording_path = row["path"]
    basename, _ = os.path.splitext(os.path.basename(recording_path))
    for n, cut in enumerate(cuts):
        frequency_class = fbins[n]
        start_time = cut.start
        min_freq = cut.min
        end_time = cut.end
        max_freq = cut.max
        bounds = [start_time, min_freq, end_time, max_freq]

        start_datetime = atime + datetime.timedelta(seconds=start_time)
        piece_time_raw = start_datetime.strftime(format=time_format)
        chunck_basename = '%.2f_%.2f_%.2f_%.2f' % tuple(bounds)
        chunck_file = f'{basename}_{chunck_basename}.npz'

        npz_path = os.path.join(write_config["write_dir"], chunck_file)

        output = {
             "bounds": np.array(bounds),
             "recording_path": np.array([recording_path]),
             "time_raw": np.array([piece_time_raw]),
             "time_format": np.array([time_format]),
             "time_zone": np.array([time_zone]),
             "array": feature_cuts[n]
        }

        output.update(include_meta)

        np.savez_compressed(npz_path, **output)

        new_row = {}
        new_row["recording_id"] = recording_id
        new_row["npz_path"] = npz_path
        new_row['start_time'] = start_time
        new_row['end_time'] = end_time
        new_row['min_freq'] = max_freq
        new_row['max_freq'] = min_freq
        new_row['time_raw'] = piece_time_raw
        new_row['time_format'] = time_format
        new_row['time_zone'] = time_zone

        write_results.append(new_row)

    return pd.DataFrame(write_results, columns=["recording_id", "npz_path",
                                                "start_time", "end_time",
                                                "min_freq", "max_freq",
                                                "time_raw", "time_format",
                                                "time_zone"])


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


@transition(name='slice_samples', outputs=["slice_results"], persist=True,
            signature=((DaskDataFramePlace, DictPlace, DictPlace),
                       (DaskDataFramePlace,)))
def slice_timed_samples(recordings, slice_config, write_config):
    """Produce feature slices dataframe."""

    meta = [('recording_id', np.dtype(int)),
            ('npz_path', np.dtype('<U')),
            ('start_time', np.dtype('float64')),
            ('end_time', np.dtype('float64')),
            ('min_freq', np.dtype('float64')),
            ('max_freq', np.dtype('float64')),
            ('time_raw', np.dtype('<U')),
            ('time_format', np.dtype('<U')),
            ('time_zone', np.dtype('<U'))]

    return recordings.audio.apply(write_timed_grid_slices,
                                  meta=meta,
                                  slice_config=slice_config,
                                  write_config=write_config)
