NUMPY_METHODS = [
    "all",
    "any",
    "argmax",
    "argmin",
    "argpartition",
    "argsort",
    "astype",
    "byteswap",
    "choose",
    "clip",
    "compress",
    "conj",
    "conjugate",
    "cumprod",
    "cumsum",
    "diagonal",
    "dot",
    "dump",
    "dumps",
    "fill",
    "flatten",
    "getfield",
    "item",
    "itemset",
    "max",
    "mean",
    "min",
    "newbyteorder",
    "nonzero",
    "partition",
    "prod",
    "ptp",
    "put",
    "ravel",
    "repeat",
    "reshape",
    "resize",
    "round",
    "searchsorted",
    "setfield",
    "setflags",
    "sort",
    "squeeze",
    "std",
    "sum",
    "swapaxes",
    "take",
    "tobytes",
    "tofile",
    "tolist",
    "tostring",
    "trace",
    "transpose",
    "var",
    "view",
    "__abs__",
    "__add__",
    "__and__",
    "__bool__",
    "__contains__",
    "__delitem__",
    "__divmod__",
    "__eq__",
    "__float__",
    "__floordiv__",
    "__ge__",
    "__getitem__",
    "__gt__",
    "__iadd__",
    "__iand__",
    "__ifloordiv__",
    "__ilshift__",
    "__imatmul__",
    "__imod__",
    "__imul__",
    "__index__",
    "__int__",
    "__invert__",
    "__ior__",
    "__ipow__",
    "__irshift__",
    "__isub__",
    "__iter__",
    "__itruediv__",
    "__ixor__",
    "__le__",
    "__len__",
    "__lshift__",
    "__lt__",
    "__matmul__",
    "__mod__",
    "__mul__",
    "__ne__",
    "__neg__",
    "__or__",
    "__pos__",
    "__pow__",
    "__radd__",
    "__rand__",
    "__rdivmod__",
    #  "__repr__",
    "__rfloordiv__",
    "__rlshift__",
    "__rmatmul__",
    "__rmod__",
    "__rmul__",
    "__ror__",
    "__rpow__",
    "__rrshift__",
    "__rshift__",
    "__rsub__",
    "__rtruediv__",
    "__rxor__",
    "__setitem__",
    "__str__",
    "__sub__",
    "__truediv__",
    "__xor__",
]


NUMPY_PROPERTIES = [
    "T",
    "data",
    "dtype",
    "flags",
    "flat",
    "imag",
    "real",
    "size",
    "itemsize",
    "nbytes",
    "ndim",
    "shape",
    "strides",
    "ctypes",
    "base",
]


class NumpyMixin:
    @property
    def array(self):
        """Get media contents."""
        return self.content

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Use numpy universal functions on media array."""
        modified_inputs = tuple(
            [
                inp.array if isinstance(inp, NumpyMixin) else inp
                for inp in inputs
            ]
        )
        modified_kwargs = {
            key: value.array if isinstance(value, NumpyMixin) else value
            for key, value in kwargs.items()
        }

        return getattr(ufunc, method)(*modified_inputs, **modified_kwargs)


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
    setattr(NumpyMixin, meth, _build_method(meth))


for prop in NUMPY_PROPERTIES:
    setattr(NumpyMixin, prop, _build_property(prop))
