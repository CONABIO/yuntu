class lazy_property:
    """
    Lazy property.
    """

    def __init__(self, func):
        self.load = func
        self.__doc__ = getattr(func, "__doc__")

    def __set_name__(self, owner, name):
        self.private_name = f"__{name}"
        self.loaded_name = f"has_{name}"
        self.clean_name = f"clean_{name}"

        setattr(owner, self.loaded_name, lambda obj: self.is_loaded(obj))
        setattr(owner, self.clean_name, lambda obj: self.clean(obj))

    def __get__(self, obj, objtype=None):
        try:
            return getattr(obj, self.private_name)

        except AttributeError:
            if not self.is_loaded(obj):
                value = self.load(obj)
                self.__set__(obj, value)

            return value

    def __set__(self, obj, value):
        setattr(obj, self.private_name, value)

    def clean(self, obj):
        delattr(obj, self.private_name)

    def is_loaded(self, obj):
        if not hasattr(obj, self.private_name):
            return False

        return getattr(obj, self.private_name) is not None
