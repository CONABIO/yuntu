from yuntu.core.utils.plugins import PluginMount


def test_plugin_system():
    class A(metaclass=PluginMount):
        pass

    assert hasattr(A, "plugins")
    assert isinstance(A.plugins, list)
    assert len(A.plugins) == 0

    class B(A):
        pass

    assert len(A.plugins) == 1
    assert B in A.plugins