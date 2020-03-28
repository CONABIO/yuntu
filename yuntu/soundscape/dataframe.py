"""Pandas accesor for soundscape methods."""
import numpy as np
import pandas as pd


@pd.api.extensions.register_dataframe_accessor("soundscape")
class SoundscapeAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if 'id' not in obj.columns or \
           'start_time' not in obj.columns or \
           'end_time' not in obj.columns or \
           'max_freq' not in obj.columns or \
           'min_freq' not in obj.columns or \
           'weight' not in obj.columns:
            raise AttributeError("Not a soundscape dataframe!.")
        index_columns = list(set(obj.columns) -
                             set(['id', 'window', 'weight']))
        for col in index_columns:
            if obj[col].dtype != np.float64 and obj[col].dtype != np.float32:
                raise ValueError("Index columns must be of type float.")
        print("Access soundscape methods.")

    def plot(self):
        pass
