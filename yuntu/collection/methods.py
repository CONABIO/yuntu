from yuntu.collection.base import Collection, TimedCollection
from yuntu.collection.irekua import IrekuaRESTCollection

def collection(col_type="simple", **kwargs):
    if col_type == "simple":
        return Collection(**kwargs)
    elif col_type == "timed":
        return TimedCollection(**kwargs)
    elif col_type == "irekua":
        return IrekuaRESTCollection(**kwargs)
    raise NotImplementedError(f"Collection type {col_type} unknown")
