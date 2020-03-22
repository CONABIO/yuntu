"""
Annotated Object Module.

This module defines a Mixin that can be given to all
objects that posses annotations.
"""


class AnnotationList:
    def __init__(self, media, annotations):
        self.media = media

        if annotations is None:
            annotations = []

        self.annotations = annotations

    def add_annotation(self, annotation):
        self.annotations.append(annotation)

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
