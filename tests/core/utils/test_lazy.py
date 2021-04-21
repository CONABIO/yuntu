from yuntu.core.utils.lazy import lazy_property


def test_lazy_property():
    class A:
        @lazy_property
        def content(self):
            return 10

        @lazy_property
        def other(self):
            return 30

    a = A()

    assert not a.has_content()
    assert a.content == 10
    assert a.has_content()

    assert not a.has_other()
    assert a.other == 30
    assert a.has_other()
