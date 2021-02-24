import numpy as np
from pony.orm import db_session

from yuntu.utils import module_object

from yuntu import Audio
from yuntu.collection.base import collection

from yuntu.core.pipeline.places import *
from yuntu.core.pipeline import transition


def write_probe_outputs(partition, probe_config, col_config, write_config, batch_size, overwrite=False):
    """Run probe on partition and write results"""

    col = collection(**col_config)

    with db_session:
        dataframe = col.get_recording_dataframe(query=partition["query"],
                                                offset=partition["offset"],
                                                limit=partition["limit"],
                                                with_annotations=True,
                                                with_metadata=True)
    col.db_manager.db.disconnect()

    probe_class = module_object(probe_config["module"])
    probe_kwargs = probe_config["kwargs"]

    rows = []
    count = 0
    with probe_class(**probe_kwargs) as probe:
        for rid, path, duration, timeexp in dataframe[["id", "path", "duration", "timeexp"]].values:
            new_row = {"id": rid}
            out_path = os.path.join(write_config["write_dir"], f"{rid}.npy")

            if os.path.exists(out_path) and not overwrite:
                raw_output = np.load(out_path)
            else:
                with Audio(path=path, timeexp=timeexp) as audio:
                    raw_output = probe.predict(audio, batch_size)
                    np.save(out_path, raw_output)

            new_row["probe_output"] = out_path

            count += 1

            if count % 10 == 0:
                gc.collect()

            rows.append(new_row)

    return rows


def insert_probe_annotations(partition, probe_config, col_config, batch_size, overwrite=False):
    """Run probe on partition and write results"""

    col = collection(**col_config)

    with db_session:
        dataframe = col.get_recording_dataframe(query=partition["query"],
                                                offset=partition["offset"],
                                                limit=partition["limit"],
                                                with_annotations=True,
                                                with_metadata=True)

    probe_class = module_object(probe_config["module"])
    probe_kwargs = probe_config["kwargs"]

    rows = []
    count = 0
    with probe_class(**probe_kwargs) as probe:
        for rid, path, duration, timeexp in dataframe[["id", "path", "duration", "timeexp"]].values:
            new_row = {"id": rid}
            with Audio(path=path, timeexp=timeexp) as audio:
                annotations = probe.annotate(audio, batch_size)

            recording = col.recordings(rid)
            for i in range(len(annotations)):
                annotations[i]["recording"] = recording

            col.annotate(annotations)

            new_row["annotations"] = annotations

            count += 1

            if count % 10 == 0:
                gc.collect()

            rows.append(new_row)

    col.db_manager.db.disconnect()

    return rows


@transition(name="probe_write", outputs=["write_result"], persist=True,
            signature=((DynamicPlace, DictPlace, DictPlace, DictPlace, ScalarPlace), (DaskDataFramePlace,)))
def probe_write(partitions, probe_config, col_config, write_config, batch_size):
    """Run probe and write results for each partition in parallel"""

    results = partitions.map(write_probe_outputs, col_config=col_config, write_config=write_config, batch_size=batch_size).flatten()

    meta = [('id', np.dtype('int')), ('probe_output', np.dtype('<U'))]

    return results.to_dataframe(meta=meta)


@transition(name="probe_annotate", outputs=["annotation_result"], persist=True,
            signature=((DynamicPlace, DictPlace, DictPlace, ScalarPlace), (DaskDataFramePlace,)))
def probe_annotate(partitions, probe_config, col_config, batch_size):
    """Run probe and annotate recordings for each partition in parallel"""

    results = partitions.map(insert_probe_annotations, col_config=col_config, batch_size=batch_size).flatten()

    meta = [('id', np.dtype('int')), ('annotations', np.dtype('O'))]

    return results.to_dataframe(meta=meta)
