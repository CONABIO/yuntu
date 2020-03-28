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
        annotation.target = self
        self.annotations.append(annotation)

    def to_dataframe(self):
        """Produce pandas DataFrame from AnnotationList."""
        data = []
        for annotation in self.annotations:
            row = {
                'id': annotation.id,
                'type': type(annotation).__name__,
                'start_time': annotation.geometry.bounds[0],
                'end_time': annotation.geometry.bounds[2],
                'min_freq': annotation.geometry.bounds[1],
                'max_freq': annotation.geometry.bounds[3]
            }
            for label in annotation.iter_labels():
                row[label.key] = label.value
            row['geometry'] = annotation.geometry
            data.append(row)
        return pd.DataFrame(data)

    def plot(self, ax=None, **kwargs):
        """Plot all annotations."""
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=kwargs.get('figsize', (15, 5)))

        key = kwargs.get('key', None)
        for annotation in self.annotations:
            if not annotation.has_label(key, mode=kwargs.get('filter', 'all')):
                continue
            annotation.plot(ax=ax, **kwargs)

        if kwargs.get('legend', False):
            ax.legend()

        return ax

    def buffer(self, buffer=None, **kwargs):
        annotations = [
            annotation.buffer(buffer=buffer, **kwargs)
            for annotation in self.annotations]
        return AnnotationList(self.media, annotations)

    def apply(self, func):
        annotations = [
            func(annotation) for annotation
            in self.annotations]
        return AnnotationList(self.media, annotations)

    def filter(self, func):
        """Return new AnnotationList with filtered annotations."""
        annotations = [
            annotation for annotation in self.annotations
            if func(annotation)]
        return AnnotationList(self.media, annotations)

    def __getitem__(self, key):
        return self.annotations[key]

    def __len__(self):
        return len(self.annotations)

    def __iter__(self):
        for annotation in self.annotations:
            yield annotation


class AnnotatedObject:
    annotation_list_class = AnnotationList

    def __init__(self, annotations=None, **kwargs):
        filtered_annotations = self.filter_annotations(annotations)
        self.annotations = self.annotation_list_class(
            self,
            filtered_annotations)

    def filter_annotations(self, annotation_list):
        if not hasattr(self, 'window'):
            return annotation_list

        if self.window is None:
            return annotation_list

        if self.window.is_trivial():
            return annotation_list

        filtered = [
            annotation for annotation in annotation_list
            if annotation.intersects(self.window)
        ]

        return filtered

    def annotate(self, annotation):
        self.annotations.add_annotation(annotation)
