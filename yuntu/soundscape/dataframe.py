"""Dataframe accesors for soundscape methods"""
import numpy as np
import pandas as pd

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
        for col, dtype in zip(list(self._obj.columns), list(self._obj.dtypes)):
            if pd.api.types.is_float_dtype(dtype) and col not in REQUIRED_SOUNDSCAPE_COLUMNS:
                self.index_columns.append(col)
        self.index_columns = list(set(self.index_columns))

        if len(self.index_columns) == 0:
            message = "Could not find any column to treat as an acoustic index."
            raise ValueError(message)

        self._is_crono = True
        for col in CRONO_SOUNDSCAPE_COLUMNS:
            if col not in self._obj.columns:
                self._is_crono = False

    def plot(self, group_col=None, crono_conf=None, ax=None, **kwargs):
        """Plot soundscape in various presentations."""
        if not self._is_crono and crono_conf is not None:
            raise ValueError("This soundscape is not cronological.")
        pass
