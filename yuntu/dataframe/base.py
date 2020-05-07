"""Audio dataframe base classes.

An audio dataframe is a
"""
import pandas as pd

from yuntu.core.audio.audio import Audio


PATH = 'path'
SAMPLERATE = 'samplerate'
TIME_EXPANSION = 'timeexp'
DURATION = 'duration'
MEDIA_INFO = 'media_info'
METADATA = 'metadata'
ID = 'id'
REQUIRED_AUDIO_COLUMNS = [
    PATH,
]
OPTIONAL_AUDIO_COLUMNS = [
    SAMPLERATE,
    TIME_EXPANSION,
    DURATION,
    MEDIA_INFO,
    METADATA,
    ID,
]


@pd.api.extensions.register_dataframe_accessor("audio")
class AudioAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._optional_columns = self._get_optional_columns(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not all(column in obj.columns for column in REQUIRED_AUDIO_COLUMNS):
            raise AttributeError("Must have 'path'")

    @staticmethod
    def _get_optional_columns(obj):
        return [
            col for col in OPTIONAL_AUDIO_COLUMNS
            if col in obj.columns]

    def _build_audio_from_row(self, row, lazy=True):
        data = {PATH: row[PATH]}

        for col in self._optional_columns:
            data[col] = row[col]

        return Audio(**data, lazy=lazy)

    def _build_audio_from_tuple(self, row, lazy=True):
        data = {PATH: getattr(row, PATH)}

        for col in self._optional_columns:
            data[col] = getattr(row, col)

        return Audio(**data, lazy=lazy)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._build_audio_from_row(self._obj.iloc[key])

        return [
            self._build_audio_from_tuple(row)
            for row in self._obj[key].itertuples()]
