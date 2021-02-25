from yuntu.core.pipeline.base import Pipeline
from yuntu.core.pipeline.places.extended import place

from yuntu.soundscape.transitions.basic_trans import load_datastores, pg_init_database

class DatastoreLoad(Pipeline):
    """Pipeline to initialize a collection and insert data from datastores."""
    _partitions = None

    def __init__(self,
                 name,
                 datastore_configs,
                 collection_config,
                 admin_config,
                 **kwargs):
        if not isinstance(collection_config, dict):
            raise ValueError(
                "Argument 'collection_config' must be a dictionary.")
        if not isinstance(admin_config, dict):
            raise ValueError(
                "Argument 'admin_config' must be a dictionary.")
        if not isinstance(datastore_configs, list):
            raise ValueError(
                "Argument 'datastore_configs' must be a list.")

        super().__init__(name, **kwargs)

        self.admin_config = admin_config
        self.collection_config = collection_config
        self.datastore_configs = datastore_configs
        self.build()

    def build(self):
        self["init_config"] = place(self.collection_config, 'dict', 'init_config')
        self["admin_config"] = place(self.admin_config, 'dynamic', 'admin_config')
        self["datastore_configs"] = place(self.datastore_configs, 'pickleable', 'datastore_configs')

        self["col_config"] = pg_init_database(self["init_config"],
                                              self["admin_config"])
        self["insert_results"] = load_datastores(self["col_config"],
                                                 self["datastore_configs"])


class DatastoreLoadPartitioned(Pipeline):
    """Pipeline to initialize a collection and insert data from datastores."""
    _partitions = None

    def __init__(self,
                 name,
                 datastore_config,
                 collection_config,
                 admin_config,
                 **kwargs):
        if not isinstance(collection_config, dict):
            raise ValueError(
                "Argument 'collection_config' must be a dictionary.")
        if not isinstance(admin_config, dict):
            raise ValueError(
                "Argument 'admin_config' must be a dictionary.")
        if not isinstance(datastore_configs, list):
            raise ValueError(
                "Argument 'datastore_configs' must be a list.")

        super().__init__(name, **kwargs)

        self.admin_config = admin_config
        self.collection_config = collection_config
        self.datastore_configs = datastore_configs
        self.build()

    def build(self):
        self["init_config"] = place(self.collection_config, 'dict', 'init_config')
        self["admin_config"] = place(self.admin_config, 'dynamic', 'admin_config')
        self["datastore_config"] = place(self.datastore_config, 'dynamic', 'datastore_config')
        self['npartitions'] = place(1, 'scalar', 'npartitions')

        self["datastore_configs"] = source_partition(self["datastore_config"],
                                                     self['npartitions'])
        self["col_config"] = pg_init_database(self["init_config"],
                                              self["admin_config"])
        self["insert_results"] = load_datastores(self["col_config"],
                                                 self["datastore_configs"])
