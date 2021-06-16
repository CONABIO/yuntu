#from yuntu.collection.base import Collection, TimedCollection
from yuntu.collection.base import ImageCollection, TimedImageCollection
#from yuntu.collection.irekua import IrekuaRESTCollection
from yuntu.collection.irekua import IrekuaRESTImageCollection

def collection(col_type="simple", **kwargs):
    if col_type == "simple":
        return Collection(**kwargs)
    elif col_type == "timed":
        return TimedCollection(**kwargs)
    elif col_type == "irekua":
        return IrekuaRESTCollection(**kwargs)
    raise NotImplementedError(f"Collection type {col_type} unknown")

def image_collection(col_type="simple", **kwargs):
    if col_type == "simple":
        return ImageCollection(**kwargs)
    elif col_type == "timed":
        return TimedImageCollection(**kwargs)
    elif col_type == "irekua":
        return IrekuaRESTImageCollection(**kwargs)
    raise NotImplementedError(f"Collection type {col_type} unknown")
