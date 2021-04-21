import numpy as np


from yuntu.core.media.base import Media
from yuntu.core.media.mixins import NumpyMixin


def test_numpy_media():
    content = np.array([0, 1, 2, 3])

    class NumpyMedia(NumpyMixin, Media):
        pass

    n = NumpyMedia(content=content)

    assert isinstance(n.array, np.ndarray)
    assert n.size == 4
    assert n.ndim == 1
    assert len(n) == 4
    assert n.shape == (4,)
    assert n.sum() == 6
    assert n.mean() == 6 / 4
    assert np.maximum(content, 2).sum() == 9
    assert (n < 2).sum() == 2
    assert (n + 2).sum() == 14
    assert n.astype(bool).sum() == 3
