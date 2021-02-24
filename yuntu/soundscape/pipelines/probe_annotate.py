from yuntu.core.pipeline.base import Pipeline
from yuntu.core.pipeline.places.extended import place

from yuntu.soundscape.transitions.basic_trans import get_partitions
from yuntu.soundscape.transitions.probe_trans import probe_annotate


class ProbeAnnotatePipeline(Pipeline):
    """Pipeline to build tfrecords from bat call annotations."""

    def __init__(self,
                 name,
                 probe_config,
                 collection_config,
                 query=None,
                 **kwargs):
        if not isinstance(default_partitions, int):
            raise ValueError("Arugment 'npartitions' must be an integer.")
        if not isinstance(collection_config, dict):
            raise ValueError("Argument 'collection_config' must be a dictionary.")
        if not isinstance(write_config, dict):
            raise ValueError("Argument 'write_config' must be a dictionary.")
        if not isinstance(probe_config, dict):
            raise ValueError("Argument 'probe_config' must be a dictionary.")

        super().__init__(name, **kwargs)

        self.default_partitions = default_partitions
        self.query = query
        self.collection_config = collection_config
        self.probe_config = probe_config
        self.build()

    def build(self):
        self["col_config"] = place(self.collection_config, 'dict', 'col_config')
        self["query"] = place(self.query, 'dynamic', 'query')
        self["npartitions"] = place(1, 'scalar', 'npartitions')
        self["overwrite"] = place(False, "dynamic", "overwrite")
        self["batch_size"] = place(200, 'scalar', 'batch_size')
        self["probe_config"] = place(self.probe_config, 'dict', 'probe_config')

        self["dir_exists"] = init_write_dir(self["write_config"],
                                            self["overwrite"])
        self["partitions"] = get_partitions(self["col_config"],
                                            self["query"],
                                            self["npartitions"],
                                            self["dir_exists"])
        self["annotation_result"] = probe_annotate(self["partitions"],
                                                   self["probe_config"],
                                                   self["col_config"],
                                                   self["batch_size"])
