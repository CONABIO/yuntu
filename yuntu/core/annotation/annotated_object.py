"""
Annotated Object Module.

This module defines a Mixin that can be given to all
objects that posses annotations.
"""
import pandas as pd


class AnnotationList:
    def __init__(self, media, annotations):
        self.media = media

        if annotations is None:
            annotations = []

        self.annotations = annotations

    def add_annotation(self, annotation):
        """Append annotation to AnnotationList."""
        self.annotations.append(annotation)

    def to_dataframe(self):
        """Produce pandas DataFrame from AnnotationList."""
        data = []
        for annotation in self.annotations:
            row = {'id': annotation.id,
                   'type': type(annotation).__name__,
                   'start_time': annotation.geometry.bounds[0],
                   'end_time': annotation.geometry.bounds[2],
                   'min_freq': annotation.geometry.bounds[1],
                   'max_freq': annotation.geometry.bounds[3]
                   }
            for label in annotation.iter_labels:
                row[label.key] = label.value
            row['geometry'] = annotation.geometry
            data.append(row)
        return pd.DataFrame(data)

    def filter(self, filter_func):
        """Return new AnnotationList with filtered annotations."""
        result = filter(filter_func, self.annotations)
        return AnnotationList(self.media, result)

    def plot(self, *args, **kwargs):
        """Plot all annotations."""
        # TODO: Should use yuntu.core.atlas.geometry.plot_geometry to
        # plot annotation geometry with all labels (?)

    def __iter__(self):
        for annotation in self.annotations:
            yield annotation


class AnnotatedObject:
    annotation_list_class = AnnotationList

    def __init__(self, *args, annotations=None, **kwargs):

        filtered_annotations = self.filter_annotations(annotations)
        self.annotations = self.annotation_list_class(self, filtered_annotations)
        super().__init__(*args, **kwargs)

    def filter_annotations(self, annotation_list):
        if not hasattr(self, 'window'):
            return annotation_list

        if self.window is None:
            return annotation_list

        filtered = [
            annotation for annotation in annotation_list
            if annotation.intersects(self.window)
        ]

        return filtered

    def annotate(self, annotation):
        self.annotations.add_annotation(annotation)
