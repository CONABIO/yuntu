"""Media module.

This module defines the base class for all media objects in yuntu.
A media object is any object that holds information on an acoustic event.
This could be the full wav array, the zero crossing rate or the spectrogram.
These media objects can all be stored and read from the filesystem.
"""
import os
from abc import ABC
from abc import abstractmethod

from yuntu.core.windows import Window
from yuntu.core.annotation.annotated_object import AnnotatedObject


class Media(ABC, AnnotatedObject):
    """Media class.

    This is the base class for all media objects in yuntu.
    """

    window_class = Window

    # pylint: disable=no-self-use
    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Use numpy universal functions on media array."""
        modified_inputs = tuple([
            inp.array
            if isinstance(inp, Media) else inp
            for inp in inputs
        ])
        modified_kwargs = {
            key:
                value.array
                if isinstance(value, Media)
                else value
            for key, value in kwargs.items()
        }

        return getattr(ufunc, method)(*modified_inputs, **modified_kwargs)

    # pylint: disable=super-init-not-called, unused-argument
    def __init__(
            self,
            path=None,
            lazy=False,
            array=None,
            window=None,
            **kwargs):
        """Construct a media object."""
        self.path = path
        self.lazy = lazy

        if window is None:
            # pylint: disable=abstract-class-instantiated
            window = self.window_class()
        self.window = window

        if array is not None:
            self._array = array
        elif not lazy:
            self._array = self.load()

        super().__init__(**kwargs)

    def clean(self):
        """Clear media contents and free memory."""
        del self._array

    def is_empty(self):
        """Check if array has not been loaded yet."""
        return not hasattr(self, '_array') or self._array is None

    def path_exists(self):
        """Determine if the media file exists in the filesystem."""
        if self.path is None:
            return False

        return os.path.exists(self.path)

    @property
    def path_ext(self):
        """Get extension of media file."""
        _, ext = os.path.splitext(self.path)
        return ext

    @abstractmethod
    def load(self):
        """Read media object into memory."""

    @abstractmethod
    def write(self, path=None, **kwargs):
        """Write media object into filesystem."""

    @abstractmethod
    def to_dict(self):
        """Return a dictionary holding all media metadata."""

    @abstractmethod
    def plot(self, ax=None, **kwargs):  # pylint: disable=invalid-name
        """Plot a representation of the media object."""

    @property
    def array(self):
        """Get media contents."""
        if self.is_empty():
            self._array = self.load()
        return self._array

    def copy(self, *args, **kwargs):
        """Copy media element."""
        cls = type(self)
        new_kwargs = self.to_dict()

        if not self.is_empty():
            new_kwargs['array'] = self.array.copy(*args, **kwargs)

        new_kwargs['window'] = self.window
        return cls(**new_kwargs)

    def __copy__(self):
        """Copy media element."""
        cls = type(self)
        kwargs = self.to_dict()

        if not self.is_empty():
            kwargs['array'] = self.array

        return cls(**kwargs)


NUMPY_METHODS = [
    'all',
    'any',
    'argmax',
    'argmin',
    'argpartition',
    'argsort',
    'astype',
    'byteswap',
    'choose',
    'clip',
    'compress',
    'conj',
    'conjugate',
    'cumprod',
    'cumsum',
    'diagonal',
    'dot',
    'dump',
    'dumps',
    'fill',
    'flatten',
    'getfield',
    'item',
    'itemset',
    'max',
    'mean',
    'min',
    'newbyteorder',
    'nonzero',
    'partition',
    'prod',
    'ptp',
    'put',
    'ravel',
    'repeat',
    'reshape',
    'resize',
    'round',
    'searchsorted',
    'setfield',
    'setflags',
    'sort',
    'squeeze',
    'std',
    'sum',
    'swapaxes',
    'take',
    'tobytes',
    'tofile',
    'tolist',
    'tostring',
    'trace',
    'transpose',
    'var',
    'view',
    '__abs__',
    '__add__',
    '__and__',
    '__bool__',
    '__contains__',
    '__delitem__',
    '__divmod__',
    '__eq__',
    '__float__',
    '__floordiv__',
    '__ge__',
    '__getitem__',
    '__gt__',
    '__iadd__',
    '__iand__',
    '__ifloordiv__',
    '__ilshift__',
    '__imatmul__',
    '__imod__',
    '__imul__',
    '__index__',
    '__int__',
    '__invert__',
    '__ior__',
    '__ipow__',
    '__irshift__',
    '__isub__',
    '__iter__',
    '__itruediv__',
    '__ixor__',
    '__le__',
    '__len__',
    '__lshift__',
    '__lt__',
    '__matmul__',
    '__mod__',
    '__mul__',
    '__ne__',
    '__neg__',
    '__or__',
    '__pos__',
    '__pow__',
    '__radd__',
    '__rand__',
    '__rdivmod__',
    '__repr__',
    '__rfloordiv__',
    '__rlshift__',
    '__rmatmul__',
    '__rmod__',
    '__rmul__',
    '__ror__',
    '__rpow__',
    '__rrshift__',
    '__rshift__',
    '__rsub__',
    '__rtruediv__',
    '__rxor__',
    '__setitem__',
    '__str__',
    '__sub__',
    '__truediv__',
    '__xor__'
]


NUMPY_PROPERTIES = [
    'T',
    'data',
    'dtype',
    'flags',
    'flat',
    'imag',
    'real',
    'size',
    'itemsize',
    'nbytes',
    'ndim',
    'shape',
    'strides',
    'ctypes',
    'base',
]


def _build_method(method_name):
    def class_method(self, *args, **kwargs):
        return getattr(self.array, method_name)(*args, **kwargs)
    return class_method


def _build_property(property_name):
    @property
    def class_property(self):
        return getattr(self.array, property_name)
    return class_property


for meth in NUMPY_METHODS:
    setattr(Media, meth, _build_method(meth))

for prop in NUMPY_PROPERTIES:
    setattr(Media, prop, _build_property(prop))
