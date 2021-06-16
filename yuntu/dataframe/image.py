"""Audio dataframe base classes"""
import pandas as pd
from dask import delayed
import dask.dataframe.extensions

from yuntu.core.image.image import Image


PATH = 'path'
#SAMPLERATE = 'samplerate'
#TIME_EXPANSION = 'timeexp'
#DURATION = 'duration'
MEDIA_INFO = 'media_info'
METADATA = 'metadata'
ID = 'id'
ANNOTATIONS = 'annotations'
REQUIRED_AUDIO_COLUMNS = [
    PATH,
]
OPTIONAL_AUDIO_COLUMNS = [
#    SAMPLERATE,
#    TIME_EXPANSION,
#    DURATION,
    MEDIA_INFO,
    METADATA,
    ID,
]


@pd.api.extensions.register_dataframe_accessor("image")
class ImageAccessor:
    path_column = PATH
#    samplerate_column = SAMPLERATE
#    timeexp_column = TIME_EXPANSION
#    duration_column = DURATION
    media_info_column = MEDIA_INFO
    metadata_column = METADATA
    id_column = ID
    annotations_columns = ANNOTATIONS

    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not all(column in obj.columns for column in REQUIRED_AUDIO_COLUMNS):
            raise AttributeError("Must have 'path'")

    def _build_image(
            self,
            row,
            lazy=True,
            path_column=None,
#            samplerate_column=None,
#            timeexp_column=None,
#            duration_column=None,
            media_info_column=None,
            metadata_column=None,
            annotations_columns=None,
            id_column=None):

        if path_column is None:
            path_column = self.path_column

#        if samplerate_column is None:
#            samplerate_column = self.samplerate_column

#        if timeexp_column is None:
#            timeexp_column = self.timeexp_column

#        if duration_column is None:
#            duration_column = self.duration_column

        if media_info_column is None:
            media_info_column = self.media_info_column

        if metadata_column is None:
            metadata_column = self.metadata_column

        if annotations_columns is None:
            annotations_columns = self.annotations_columns

        if id_column is None:
            id_column = self.id_column

        data = {
            PATH: getattr(row, path_column),
#            SAMPLERATE: getattr(row, samplerate_column, None),
#            TIME_EXPANSION: getattr(row, timeexp_column, None),
#            DURATION: getattr(row, duration_column, None),
            MEDIA_INFO: getattr(row, media_info_column, None),
            METADATA: getattr(row, metadata_column, None),
            ID: getattr(row, id_column, None),
            ANNOTATIONS: getattr(row, annotations_columns, [])
        }

        return Image(**data, lazy=lazy)

    def apply(self, func):
        return self._obj.apply(
            lambda row: func(row, self._build_image(row)),
            axis=1)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._build_image(self._obj.iloc[key])

        return [
            self._build_image(row)
            for row in self._obj[key].itertuples()]

    def get(
            self,
            row=None,
            id=None,
            lazy=True,
            path_column=None,
#            samplerate_column=None,
#            timeexp_column=None,
#            duration_column=None,
            media_info_column=None,
            annotations_columns=None,
            metadata_column=None,
            id_column=None):
        if id_column is None:
            id_column = self.id_column

        if row is not None:
            row = self._obj.iloc[row]
        elif id is not None:
            row = self._obj[self._obj[id_column] == id].iloc[0]
        else:
            row = self._obj.iloc[0]

        return self._build_image(
            row,
            lazy=lazy,
            path_column=path_column,
#            samplerate_column=samplerate_column,
#            timeexp_column=timeexp_column,
#            duration_column=duration_column,
            media_info_column=media_info_column,
            metadata_column=metadata_column,
            annotations_columns=annotations_columns,
            id_column=id_column)

    def change_path_column(self, new_column):
        self.path_column = new_column

#    def change_samplerate_column(self, new_column):
#        self.samplerate_column = new_column

#    def change_timeexp_column(self, new_column):
#        self.timeexp_column = new_column

#    def change_duration_column(self, new_column):
#        self.duration_column = new_column

    def change_media_info_column(self, new_column):
        self.media_info_column = new_column

    def change_metadata_column(self, new_column):
        self.metadata_column = new_column

    def change_annotations_column(self, new_column):
        self.annotations_columns = new_column

    def change_id_column(self, new_column):
        self.id_column = new_column


def dask_wrapper(func):
    name = func.__name__

    def wrapper(self, *args, **kwargs):

        def delayed_func():
            accesor = self._obj.compute().image
            method = getattr(accesor, name)
            return method(*args, **kwargs)

        return delayed(delayed_func)()
    return wrapper


@dask.dataframe.extensions.register_dataframe_accessor("image")
class DaskImageAccesor(ImageAccessor):
    @dask_wrapper
    def __getitem__(self, key):
        super().__getitem__(key)

    def apply(self, func, args=(), meta='__no_default__', **kwargs):
        def wrapper(row, *nargs, **kwargs):
            image = self._build_image(row)
            return func(row, image, *nargs, **kwargs)

        return self._obj.apply(
            wrapper,
            axis=1,
            args=args,
            meta=meta,
            **kwargs)

    # pylint: disable=redefined-builtin, too-many-arguments
    @dask_wrapper
    def get(
            self,
            row=None,
            id=None,
            lazy=True,
            path_column=None,
#            samplerate_column=None,
#            timeexp_column=None,
#            duration_column=None,
            media_info_column=None,
            metadata_column=None,
            id_column=None):
        return super().get(
            row=None,
            id=None,
            lazy=True,
            path_column=None,
#            samplerate_column=None,
#            timeexp_column=None,
#            duration_column=None,
            media_info_column=None,
            metadata_column=None,
            id_column=None)
