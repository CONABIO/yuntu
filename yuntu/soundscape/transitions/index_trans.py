"""Transitions for acoustic indices."""
import numpy as np
import pandas as pd
from yuntu.core.pipeline.transitions.decorators import transition
from yuntu.core.pipeline.places import PickleablePlace
from yuntu.core.pipeline.places.extended import DaskDataFramePlace
from yuntu.soundscape.dataframe import SoundscapeAccessor


def feature_indices(row, indices):
    """Compute acoustic indices for one row."""
    new_row = {}
    for index in indices:
        new_row[index.name] = index(row['feature_cut'])
    return pd.Series(new_row)


@transition(name='apply_indices', outputs=["index_results"],
            is_output=True, persist=True, keep=True,
            signature=((DaskDataFramePlace, PickleablePlace),
                       (DaskDataFramePlace, )))
def apply_indices(slices, indices):
    """Apply acoustic indices to slices."""
    index_names = [index.name for index in indices]
    if len(index_names) != len(set(index_names)):
        message = "Index names have duplicates. Please use a diferent name" + \
                  " for each index to compute."
        raise ValueError(message)

    meta = [(index.name,
            np.dtype('float64'))
            for index in indices]

    results = slices.apply(feature_indices,
                           meta=meta,
                           axis=1,
                           indices=indices)
    for index in indices:
        slices[index.name] = results[index.name]

    return slices.drop(['feature_cut'], axis=1)
