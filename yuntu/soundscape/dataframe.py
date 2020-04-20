"""Pandas accesor for soundscape methods."""
import numpy as np
import pandas as pd
from sklearn.manifold import Isomap


@pd.api.extensions.register_dataframe_accessor("soundscape")
class SoundscapeAccessor:
    def __init__(self, pandas_obj):
        self.basic_columns = ['id',
                              'start_time',
                              'end_time',
                              'max_freq',
                              'min_freq',
                              'weight']
        self.index_columns = None
        self._basic_validation(self, pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _basic_validation(self, obj):
        for col in self.basic_columns:
            if col not in obj.columns:
                message = f"Not a soundscape. Missing '{col}' column."
                raise ValueError(message)
        self.index_columns = list(set(obj.columns) -
                                  set(self.basic_columns+['hash']))
        if len(self.index_columns) == 0:
            message = "Could not find any indices."
            raise ValueError(message)
        for col in self.index_columns:
            if obj[col].dtype != np.float64 and obj[col].dtype != np.float32:
                raise ValueError("All columns must be of type float.")
        print("Access to soundscape methods")

    def isomap(self, n_components=2):
        """Produce isomap embedding of indices and return new soundscape."""
        if n_components > 3 or n_components == 0:
            message = "Argument 'n_components' must be an " + \
                      "integer between 1 and 3."
            raise ValueError(message)
        out_df = self._obj.fillna(0)
        values = out_df[self.index_columns].values
        out_cols = self.basic_columns
        if self.is_hashed():
            out_cols.append('hash')
        out_df = out_df[out_cols]
        embedding = Isomap(n_components=2)
        values_ = embedding.fit_transform(values)
        for comp in range(n_components):
            out_df[f"COMP{comp}"] = values_[:, [comp]]
        return out_df

    def is_hashed(self):
        return 'hash' in self._obj.columns
