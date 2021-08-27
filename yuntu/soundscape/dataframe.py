"""Dataframe accesors for soundscape methods"""
import numpy as np
import pandas as pd
import pytz
from yuntu.soundscape.utils import absolute_timing
from yuntu.soundscape.hashers.base import Hasher
from yuntu.soundscape.hashers.crono import CronoHasher
from yuntu.soundscape.hashers.crono import DEFAULT_HASHER_CONFIG
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
                               MIN_FREQ,
                               WEIGHT]
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
        print("Generating time reference...")
        out = self._obj[list(self._obj.columns)]
        out["abs_start_time"] = self._obj.apply(lambda x: absolute_timing(x["time_utc"], x["start_time"]), axis=1)
        out["abs_end_time"] = self._obj.apply(lambda x: absolute_timing(x["time_utc"], x["end_time"]), axis=1)
        return out

    def add_hash(self, hasher, out_name="xhash"):
        """Add row hasher"""
        print("Hashing dataframe...")
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
        return out


    def plot_sequence(self, rgb, view_time_zone="America/Mexico_city", xticks=10,
                      yticks=10, ylabel="Frequency", xlabel="Time", interpolation="bilinear",
                      time_format='%H:%M:%S', keep_timing=False, ax=None):
        if "abs_start_time" not in self._obj.columns or "abs_end_time" not in self._obj.columns:
            df = self.add_absolute_time()
            if keep_timing:
                self._obj = df
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
        all_hashes = list(np.arange(0, time_module))

        keep_hash = False
        do_hash = False
        if hash_col is None:
            hash_name = "crono_hasher"
            do_hash = True
        elif hash_col not in self._obj.columns:
            hash_name = hash_col
            keep_hash = True
            do_hash = True

        df = self._obj
        if do_hash:
            hasher = CronoHasher(**cycle_config)
            hashed_df = df.soundscape.add_hash(hasher, out_name=hash_name)
            if keep_hash:
                print(f"Keeping hash with column name '{hash_name}'")
                self._obj = hashed_df
        else:
            hashed_df = df

        missing_hashes = [x for x in all_hashes if x not in list(hashed_df[hash_name].unique())]
        nfreqs = hashed_df["max_freq"].unique().size
        hashed_df["unhashed_time"] = hashed_df[hash_name].apply(hasher.unhash)
        min_t =  hashed_df["unhashed_time"].min()
        max_t =  hashed_df["unhashed_time"].max()
        max_f = df["max_freq"].max()
        min_f = df["min_freq"].min()

        snd_matrix = (np.flip(np.reshape(hashed_df[["max_freq", "unhashed_time"]+rgb]
                                         .groupby(by=["max_freq", "unhashed_time"], as_index=False)
                                         .mean()
                                         .sort_values(by=["max_freq", "unhashed_time"])[rgb].values, [nfreqs,-1,3]),axis=0))
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
