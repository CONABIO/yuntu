
"""Transitions for basic usage."""
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.bag as db
from pony.orm import db_session

from yuntu.utils import module_object

from yuntu.core.audio.audio import Audio, MEDIA_INFO_FIELDS
from yuntu.core.database.mixins.utils import pg_create_db
from yuntu.collection.base import collection

from yuntu.core.pipeline.places import *
from yuntu.core.pipeline.transitions.decorators import transition

from yuntu.soundscape.utils import slice_windows
from yuntu.soundscape.hashers.base import Hasher
from yuntu.soundscape.dataframe import SoundscapeAccessor


def get_fragment_size(col_config, query):
    col = collection(**col_config)

    with db_session:
        fragment_length = col.recordings(query=qury).count()

    col.db_manager.db.disconnect()
    return fragment_length

def insert_datastore(dstore_config, col_config):
    dstore_class = module_object(dstore_config["module"])
    dstore_kwargs = dstore_config["kwargs"]

    datastore = dstore_class(**dstore_kwargs)
    col = collection(**col_config)

    with db_session:
        datastore_id, recording_inserts, annotation_inserts = datastore.insert_into(col)

    col.db_manager.db.disconnect()

    return {"datastore_record": datastore_id,
            "recording_inserts": recording_inserts,
            "annotation_inserts": annotation_inserts}

@transition(name='add_hash', outputs=["hashed_soundscape"],
            keep=True, persist=True, is_output=True,
            signature=((DaskDataFramePlace, PickleablePlace, ScalarPlace),
                       (DaskDataFramePlace, )))
def add_hash(dataframe, hasher, out_name="xhash"):
    if not isinstance(hasher, Hasher):
        raise ValueError("Argument 'hasher' must be of class Hasher.")
    if not hasher.validate(dataframe):
        str_cols = str(hasher.columns)
        message = ("Input dataframe is incompatible with hasher."
                   f"Missing column inputs. Hasher needs: {str_cols} ")
        raise ValueError(message)

    meta = [(out_name, hasher.dtype)]
    result = dataframe.apply(hasher, out_name=out_name, meta=meta, axis=1)
    dataframe[out_name] = result[out_name]

    return dataframe


@transition(name='as_dd', outputs=["recordings_dd"],
            signature=((PandasDataFramePlace, ScalarPlace),
                       (DaskDataFramePlace,)))
def as_dd(pd_dataframe, npartitions):
    """Transform audio dataframe to a dask dataframe for computations."""
    dask_dataframe = dd.from_pandas(pd_dataframe,
                                    npartitions=npartitions,
                                    name="as_dd")
    return dask_dataframe


@transition(name="get_partitions", outputs=["partitions"],
            signature=((DictPlace, DynamicPlace, ScalarPlace, DynamicPlace), (DynamicPlace,)))
def get_partitions(col_config, query, npartitions=1, proceed=True):
    if not proceed:
        raise InterruptedError(f"Interrupted by pipeline checkpoint.")

    length = get_fragment_size(col_config, query)
    if length == 0:
        raise ValueError("Collection has no data. Populate collection first.")

    psize = int(np.floor(length / npartitions))
    psize = min(length, max(psize, 1))

    partitions = []
    for ind in range(0, length, psize):
        offset = ind
        limit = min(psize, length - offset)

        stop = False
        if length - (offset + limit) < 30:
            limit = length - offset
            stop = True

        partitions.append({"query": query, "limit": limit, "offset": offset})

        if stop:
            break


    return db.from_sequence(partitions, npartitions=len(partitions))


@transition(name="init_write_dir", outputs=["dir_exists"],
            signature=((DictPlace, DynamicPlace), (DynamicPlace,)))
def init_write_dir(write_config, overwrite=False):
    """Initialize output directory"""

    if os.path.exists(write_config["write_dir"]) and overwrite:
        shutil.rmtree(write_config["write_dir"], ignore_errors=True)
    if not os.path.exists(write_config["write_dir"]):
        os.makedirs(write_config["write_dir"])
    return True


@transition(name="pg_init_database", outputs=["col_config"],
            signature=((DictPlace, DynamicPlace), (DictPlace,)))
def pg_init_database(init_config, admin_config):
    pg_create_db(init_config["db_config"]["config"],
                 admin_user=admin_config["admin_user"],
                 admin_password=admin_config["admin_password"],
                 admin_db=admin_config["admin_db"])

    col = collection(**init_config)
    col.db_manager.db.disconnect()

    return init_config


@transition(name="load_datastores", outputs=["insert_results"],
            signature=((DictPlace, PickleablePlace), (DaskDataFramePlace,)))
def load_datastores(col_config, dstore_configs):
    dstore_bag = db.from_sequence(dstore_configs, npartitions=len(dstore_configs))
    inserted = dstore_bag.map(insert_datastore, col_config=col_config)

    meta = [('datastore_record', np.dtype('int')),
            ('recording_inserts', np.dtype('int')),
            ('annotation_inserts', np.dtype('int'))]

    return inserted.to_dataframe(meta=meta)
