"""Audio dataframe base classes.

An audio dataframe is a
"""
import pandas as pd

from yuntu.core.annotation.annotation import Annotation


GEOMETRY = 'geometry'
TYPE = 'type'
LABELS = 'labels'
ID = 'id'
REQUIRED_ANNOTATION_COLUMNS = [
    GEOMETRY,
    TYPE,
]
OPTIONAL_ANNOTATION_COLUMNS = [
    LABELS,
]


@pd.api.extensions.register_dataframe_accessor("annotation")
class AnnotationAccessor:
    type_column = TYPE
    geometry_column = GEOMETRY
    labels_column = LABELS
    id_column = ID

    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        columns = obj.columns
        for column in REQUIRED_ANNOTATION_COLUMNS:
            if column not in columns:
                raise AttributeError(f"Must have column {column}")

    def _build_annotation(
            self,
            row,
            type_column=None,
            geometry_column=None,
            labels_column=None,
            id_column=None):

        if type_column is None:
            type_column = self.type_column

        if geometry_column is None:
            geometry_column = self.geometry_column

        if labels_column is None:
            labels_column = self.labels_column

        if id_column is None:
            id_column = self.id_column

        data = {
            GEOMETRY: {
                'wkt': getattr(row, geometry_column)
            },
            TYPE: getattr(row, type_column),
            LABELS: getattr(row, labels_column, []),
            ID: getattr(row, id_column, None)
        }

        return Annotation.from_dict(data)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._build_annotation(self._obj.iloc[key])

        return [
            self._build_annotation(row)
            for row in self._obj[key].itertuples()]

    def get(
            self,
            row=None,
            id=None,
            type_column=None,
            geometry_column=None,
            labels_column=None,
            id_column=None):
        if id_column is None:
            id_column = self.id_column

        if row is not None:
            row = self._obj.iloc[row]
        elif id is not None:
            row = self._obj[self._obj[id_column] == id].iloc[0]
        else:
            row = self._obj.iloc[0]

        return self._build_annotation(
            row,
            type_column=type_column,
            geometry_column=geometry_column,
            labels_column=labels_column,
            id_column=id_column)

    def change_type_column(self, new_column):
        self.type_column = new_column

    def change_geometry_column(self, new_column):
        self.geometry_column = new_column

    def change_labels_column(self, new_column):
        self.labels_column = new_column

    def change_id_column(self, new_column):
        self.id_column = new_column
