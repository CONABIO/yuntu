import datetime
from io import BytesIO
from typing import Any
from typing import TypedDict
from typing import Hashable


DateValue = TypedDict(
    "DateValue",
    {"date": datetime.datetime, "value": Any},
)


class LRUCacheMixin:
    """Least Recently Used Mixin.

    This helper class implements the least recently used algorithm for cache
    management. Cache systems inheriting from this class will select the oldest
    item in the cache for removal. If an item is retrieved from the cache it
    will be considered new.
    """

    def encode_value(self, value: Any) -> DateValue:
        """Save the value and storage date in memory."""
        return {
            "date": datetime.datetime.now(),
            "value": super().encode_value(value),
        }

    def retrieve_value(self, key: Hashable) -> Any:
        """Retrieve the stored value"""
        value = super().retrieve_value(key)
        value["date"] = datetime.datetime.now()
        return value["value"]

    def remove_one(self) -> None:
        """Remove the earliest updated item."""
        retriever = super().retrieve_value

        last = min(
            self._cache,
            key=lambda k: retriever(k)["date"],
        )

        self.remove_item(last)


class BytesIOCacheMixin:
    """Bytes IO Mixin.

    When this mixin is used the cache system will only accept and return ByteIO
    objects.
    """

    def encode_value(self, value: BytesIO) -> Any:
        data = value.read()
        value.seek(0)
        return super().encode_value(data)

    def decode_value(self, value: Any) -> BytesIO:
        value = super().decode_value(value)
        return BytesIO(value)
