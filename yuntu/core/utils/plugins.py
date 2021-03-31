# -*- coding: utf-8 -*-
"""
Plugin system for yuntu.

Yuntu is designed to be extensible. Hence a plugin system is needed
to accomodate new functionalities to the existing API. The design
has been taken from the following blog post:

    http://martyalchin.com/2008/jan/10/simple-plugin-framework/

In order to make a plugin-able interface all you need to do is
is make a class that uses the PluginMount as a metaclass. This interface
class will store a plugin list. If an implementation of the interface is
loaded, it will automatically be included into the plugin list.

Example:
    Say you want to make an interface of file loaders::

        >>> class FileLoader(metaclass=PluginMount):
        >>>     def load(self, path):
        >>>         # Implement method here


    Whenever you inherit from this class::

        >>> class WavLoader(FileLoader):
        >>>     def load(self, path):
        >>>         # ...

    It will automatically be included into the list of plugins
    from FileLoader::

        >>> WavLoader in FileLoader.plugins
        True

    The class must be loaded for it to appear in the plugin list.
"""


class PluginMount(type):
    """
    Taken from http://martyalchin.com/2008/jan/10/simple-plugin-framework/
    """

    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, "plugins"):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            cls.plugins = []
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            cls.plugins.append(cls)
