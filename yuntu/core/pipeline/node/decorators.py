"""Pipeline decorators.

This methods are intended to make operation declaration more friendly.
"""
import functools
from yuntu.core.pipeline.node.transitions import Transition
from yuntu.core.pipeline.node.tools import knit


def dd_op(name=None,
          pipeline=None,
          is_output=False,
          persist=False,
          keep=False,
          outputs=None,
          signature=None):
    """Return a dask dataframe operation.

    A dask dataframe operation returns a dataframe and has methods for saving
    and restoring parquet files to dataframe.
    """
    if signature is None:
        message = "A signature must be provided for this kind of decorator."
        raise ValueError(message)

    def wrapper(func):
        @functools.wraps(func)
        def creator(*args,
                    name=name,
                    pipeline=pipeline,
                    is_output=is_output,
                    persist=persist,
                    keep=keep,
                    outputs=outputs,
                    signature=signature,
                    **kwargs):
            all_args = list(args) + [kwargs[key] for key in kwargs]

            if outputs is not None:
                if not isinstance(outputs, list):
                    message = "Argument 'outputs' must be a tuple or a list."
                    raise ValueError(message)
                if not len(outputs) == len(signature[1]):
                    message = ("Outputs length must be equal to the length " +
                               " of the second element of 'signature'.")
                    raise ValueError(message)
                for ind, out in enumerate(outputs):
                    if not isinstance(out, str):
                        if not isinstance(out, signature[1][ind]):
                            failed_class = type(out)
                            message = f"Incompatible output {failed_class}."
                            raise ValueError(message)

            if pipeline is None:
                if len(all_args) != 0:
                    pipeline = knit(*all_args)
            else:
                name = pipeline.name
                if len(all_args) != 0:
                    pipeline = pipeline.merge(knit(*all_args))
                    pipeline.name = name

            transition_outs = []
            if outputs is not None:
                for ind, out in enumerate(outputs):
                    if isinstance(out, str):
                        node_class = signature[1][ind]
                        transition_outs.append(node_class(name=out,
                                                          pipeline=pipeline,
                                                          is_output=is_output,
                                                          persist=persist,
                                                          keep=keep))
                    else:
                        transition_outs.append(out)

            else:
                for node_class in signature[1]:
                    curr_outs = len(transition_outs)
                    out_name = f"{name}_output_{curr_outs}"
                    transition_outs.append(node_class(name=out_name,
                                                      pipeline=pipeline,
                                                      is_output=is_output,
                                                      persist=persist,
                                                      keep=keep))

            transition = Transition(name=name,
                                    pipeline=pipeline,
                                    operation=func,
                                    inputs=all_args,
                                    outputs=transition_outs,
                                    signature=signature)
            if len(transition.outputs) == 1:
                return transition.outputs[0]
            return transition.outputs
        return creator
    return wrapper
