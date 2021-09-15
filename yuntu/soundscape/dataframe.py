"""Dataframe accesors for soundscape methods"""
import numpy as np
import pandas as pd
import pytz
from dask.diagnostics import ProgressBar
from yuntu.soundscape.utils import absolute_timing
from yuntu.soundscape.hashers.base import Hasher
from yuntu.soundscape.hashers.crono import CronoHasher
from yuntu.soundscape.hashers.crono import DEFAULT_HASHER_CONFIG
from yuntu.soundscape.pipelines.build_soundscape import HashSoundscape, AbsoluteTimeSoundscape
import matplotlib.dates as mdates

import pytz
import numpy as np
import datetime


ID = 'id'
START_TIME = 'start_time'
END_TIME = 'end_time'
MAX_FREQ = 'max_freq'
MIN_FREQ = 'min_freq'
WEIGHT = 'weight'

REQUIRED_SOUNDSCAPE_COLUMNS = [ID,
                               START_TIME,
                               END_TIME,
                               MAX_FREQ,
                               MIN_FREQ]
TIME = "time_raw"
TIME_FORMAT = "time_format"
TIME_ZONE = "time_zone"

CRONO_SOUNDSCAPE_COLUMNS = [TIME, TIME_FORMAT, TIME_ZONE]

@pd.api.extensions.register_dataframe_accessor("soundscape")
class SoundscapeAccessor:
    def __init__(self, pandas_obj):
        id_column = ID
        start_time_column = START_TIME
        end_time_column = END_TIME
        max_freq_column = MAX_FREQ
        min_freq_column = MIN_FREQ
        weight_column = WEIGHT
        time_column = TIME
        time_format_column = TIME_FORMAT
        time_zone_column = TIME_ZONE

        self.index_columns = None
        self._is_crono = None
        self._validate(self, pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(self, obj):
        for col in REQUIRED_SOUNDSCAPE_COLUMNS:
            if col not in obj.columns:
                message = f"Not a soundscape. Missing '{col}' column."
                raise ValueError(message)

        self.index_columns = []
        for col, dtype in zip(list(obj.columns), list(obj.dtypes)):
            if pd.api.types.is_float_dtype(dtype) and col not in REQUIRED_SOUNDSCAPE_COLUMNS:
                self.index_columns.append(col)
        self.index_columns = list(set(self.index_columns))

        if len(self.index_columns) == 0:
            message = "Could not find any column to treat as an acoustic index."
            raise ValueError(message)

        self._is_crono = True
        for col in CRONO_SOUNDSCAPE_COLUMNS:
            if col not in obj.columns:
                self._is_crono = False

    def add_absolute_time(self):
        """Add absolute reference from UTC time"""
        print("Generating absolute time reference...")
        out = self._obj[list(self._obj.columns)]
        out["abs_start_time"] = self._obj.apply(lambda x: absolute_timing(x["time_utc"], x["start_time"]), axis=1)
        return out

    def apply_absolute_time(self, name="apply_absolute_time", work_dir="/tmp", persist=True,
                            read=False, npartitions=1, client=None, show_progress=True,
                            compute=True, time_col="start_time", out_name="abs_start_time", **kwargs):
        """Add absolute reference from UTC time"""
        print("Generating absolute time reference...")
        pipeline = AbsoluteTimeSoundscape(name=name,
                                          work_dir=work_dir,
                                          soundscape_pd=self._obj,
                                          time_col=time_col,
                                          out_name=out_name,
                                          **kwargs)
        if read:
            tpath = os.path.join(work_dir, name, "persist", "absolute_timed_soundscape.parquet")
            if not os.path.exists(tpath):
                raise ValueError(f"Cannot read soundscape. Target file {tpath} does not exist.")
            print("Reading soundscape from file...")
            return pipeline["absolute_timed_soundscape"].read().compute()

        pipeline["absolute_timed_soundscape"].persist = persist
        if compute:
            print("Computing soundscape...")
            if show_progress:
                with ProgressBar():
                    df = pipeline["absolute_timed_soundscape"].compute(client=client,
                                                                       feed={"npartitions": npartitions})
            else:
                df = pipeline["absolute_timed_soundscape"].compute(client=client,
                                                                   feed={"npartitions": npartitions})

            return df
        return pipeline["absolute_timed_soundscape"].future(client=client, feed={"npartitions": npartitions})

    def apply_hash(self, name="apply_hash", work_dir="/tmp", persist=True,
                   read=False, npartitions=1, client=None, show_progress=True,
                   compute=True, **kwargs):
        """Apply indices and produce soundscape."""
        print("Hashing dataframe...")
        pipeline = HashSoundscape(name=name,
                                  work_dir=work_dir,
                                  soundscape_pd=self._obj,
                                  **kwargs)
        if read:
            tpath = os.path.join(work_dir, name, "persist", "hashed_soundscape.parquet")
            if not os.path.exists(tpath):
                raise ValueError(f"Cannot read soundscape. Target file {tpath} does not exist.")
            print("Reading soundscape from file...")
            return pipeline["hashed_soundscape"].read().compute()

        pipeline["hashed_soundscape"].persist = persist
        if compute:
            print("Computing soundscape...")
            if show_progress:
                with ProgressBar():
                    df = pipeline["hashed_soundscape"].compute(client=client,
                                                               feed={"npartitions": npartitions})
            else:
                df = pipeline["hashed_soundscape"].compute(client=client,
                                                           feed={"npartitions": npartitions})

            return df
        return pipeline["hashed_soundscape"].future(client=client, feed={"npartitions": npartitions})

    def add_hash(self, hasher, out_name="xhash"):
        """Add row hasher"""
        print("Hashing dataframe...")
        pipeine = HashSoundscape
        if not hasher.validate(self._obj):
            str_cols = str(hasher.columns)
            message = ("Input dataframe is incompatible with hasher."
                       f"Missing column inputs. Hasher needs: {str_cols} ")
            raise ValueError(message)

        result = self._obj.apply(hasher, out_name=out_name, axis=1)
        if out_name in list(self._obj.columns):
            raise ValueError(f"Name '{out_name}' not available." +
                             "A column already has that name.")

        out = self._obj[list(self._obj.columns)]
        out[out_name] = result[out_name]
        out[f"{out_name}_time"] = out[out_name].apply(hasher.unhash)
        return out

    def plot_sequence(self, rgb, view_time_zone="America/Mexico_city", xticks=10,
                      yticks=10, ylabel="Frequency", xlabel="Time", interpolation="bilinear",
                      time_format='%H:%M:%S', ax=None):
        if "abs_start_time" not in self._obj.columns or "abs_end_time" not in self._obj.columns:
            df = self.add_absolute_time()
        else:
            df = self._obj

        utc_zone = pytz.timezone("UTC")
        local_zone = pytz.timezone(view_time_zone)
        if ax is None:
            fig, ax = plt.subplots()

        nfreqs = df["max_freq"].unique().size
        min_t = df["abs_start_time"].min()
        max_t = df["abs_end_time"].max()
        max_f = df["max_freq"].max()
        min_f = df["min_freq"].min()

        snd_matrix = np.flip(np.reshape(df.sort_values(by=["max_freq", "abs_start_time"])[rgb].values, [nfreqs,-1,3]),axis=0)

        ntimes = snd_matrix.shape[1]

        max_feature_spec = np.amax(snd_matrix, axis=(0,1))
        min_feature_spec = np.amin(snd_matrix, axis=(0,1))
        norm_feature_spec = (snd_matrix - min_feature_spec)/(max_feature_spec - min_feature_spec)

        ax.imshow(np.flip(norm_feature_spec, axis=0), aspect="auto")
        tstep = float(ntimes)/xticks
        ax.set_xticks(np.arange(0,ntimes+tstep,tstep))
        tlabel_step = (max_t - min_t) / xticks
        ax.set_xticklabels([utc_zone.localize(min_t+tlabel_step*i).astimezone(local_zone).strftime(format=time_format)
                           for i in range(xticks)]+[utc_zone.localize(max_t).astimezone(local_zone).strftime(format=time_format)])

        ax.invert_yaxis()
        fstep = float(nfreqs)/yticks
        ax.set_yticks(np.arange(0,nfreqs+fstep,fstep))
        flabel_step = float(max_f-min_f)/yticks
        yticklabels = ["{:.2f}".format(x) for x in list(np.arange(min_f/1000,(max_f+flabel_step)/1000,flabel_step/1000))]
        ax.set_yticklabels(yticklabels)

        ax.set_ylabel(f"{ylabel} (kHz)")
        ax.set_xlabel(f"{xlabel} ({time_format})")

        return ax


    def plot_cycle(self, rgb, hash_col=None, cycle_config=DEFAULT_HASHER_CONFIG, aggr="mean", xticks=10,
                   yticks=10, ylabel="Frequency", xlabel="Time", interpolation="bilinear",
                   time_format='%H:%M:%S', ax=None):
        """Plot soundscape according to cycle configs."""

        time_module = cycle_config["time_module"]
        time_unit = cycle_config["time_unit"]
        max_hash = self._obj[hash_col].max()
        all_hashes = list(np.arange(0, max_hash+1))

        do_hash = False
        hash_name = "crono_hasher"
        if hash_col is None:
            do_hash = True
        elif hash_col not in list(self._obj.columns):
            hash_name = hash_col
            do_hash = True
        else:
            hash_name = hash_col

        df = self._obj
        if do_hash:
            hasher = CronoHasher(**cycle_config)
            hashed_df = df.soundscape.add_hash(hasher, out_name=hash_name)
        else:
            hashed_df = df

        missing_hashes = [x for x in all_hashes if x not in list(hashed_df[hash_name].unique())]
        nfreqs = hashed_df["max_freq"].unique().size
        min_t =  hashed_df[f"{hash_name}_time"].min()
        max_t =  hashed_df[f"{hash_name}_time"].max()
        max_f = df["max_freq"].max()
        min_f = df["min_freq"].min()

        snd_matrix = (np.flip(np.reshape(hashed_df[["max_freq", f"{hash_name}_time"]+rgb]
                                         .groupby(by=["max_freq", f"{hash_name}_time"], as_index=False)
                                         .mean()
                                         .sort_values(by=["max_freq", f"{hash_name}_time"])[rgb].values, [nfreqs,-1,3]),axis=0))
        ntimes = snd_matrix.shape[1]

        max_feature_spec = np.amax(snd_matrix, axis=(0,1))
        min_feature_spec = np.amin(snd_matrix, axis=(0,1))
        norm_feature_spec = (snd_matrix - min_feature_spec)/(max_feature_spec - min_feature_spec)

        # Fill missing units
        null_arr = np.empty([nfreqs,3])
        null_arr[:,:] = np.NaN
        for x in missing_hashes:
            norm_feature_spec = np.insert(norm_feature_spec, x+1, null_arr, 1)

        ax.imshow(np.flip(norm_feature_spec, axis=0), aspect="auto", interpolation=interpolation)
        tstep = float(time_module)/xticks
        ax.set_xticks(np.arange(0,time_module,tstep))
        tlabel_step = datetime.timedelta(seconds=time_unit)*time_module / xticks
        start_tzone = pytz.timezone(cycle_config["start_tzone"])

        ax.set_xticklabels([(min_t+tlabel_step*i).strftime(format=time_format)
                           for i in range(xticks)])

        ax.invert_yaxis()
        fstep = float(nfreqs)/yticks
        ax.set_yticks(np.arange(0,nfreqs+fstep,fstep))
        flabel_step = float(max_f-min_f)/yticks
        yticklabels = ["{:.2f}".format(x) for x in list(np.arange(min_f/1000,(max_f+flabel_step)/1000,flabel_step/1000))]
        ax.set_yticklabels(yticklabels)

        ax.set_ylabel(f"{ylabel} (kHz)")
        ax.set_xlabel(f"{xlabel} ({time_format})")

        return ax
