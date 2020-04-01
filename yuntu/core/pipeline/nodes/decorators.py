"""Pipeline decorators.

This methods are intended to make operation declaration more friendly.
"""
import functools
from yuntu.core.pipeline.nodes.operations import DaskDataFrameOperation


def dd_op(name, pipeline=None, is_output=False, persist=False, keep=False):
    """Return a dask dataframe operation.

    A dask dataframe operation returns a dataframe and has methods for saving
    and restoring parquet files to dataframe.
    """
    def wrapper(func):
        @functools.wraps(func)
        def creator(*args,
                    pipeline=pipeline,
                    is_output=is_output,
                    persist=persist,
                    keep=keep,
                    **kwargs):
            all_args = list(args) + [kwargs[key] for key in kwargs]
            if pipeline is None:
                if len(all_args) != 0:
                    pipeline = all_args[0].pipeline
            for arg in all_args:
                if ((arg.pipeline is None
                    and pipeline is not None) or
                   (arg.pipeline is not None
                    and pipeline is None) or
                   (arg.pipeline != pipeline)):
                    raise ValueError('Nodes have different pipelines.')
            return DaskDataFrameOperation(name=name,
                                          pipeline=pipeline,
                                          operation=func,
                                          inputs=all_args,
                                          is_output=is_output,
                                          persist=persist)
        return creator
    return wrapper
