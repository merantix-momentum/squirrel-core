from squirrel.driver.store_driver import StoreDriver
from squirrel.serialization import MessagepackSerializer

__all__ = [
    "MessagepackDriver",
]


class MessagepackDriver(StoreDriver):
    """A StoreDriver that by default uses SquirrelStore with messagepack serialization."""

    name = "messagepack"

    def __init__(self, url: str, **kwargs):
        """Initializes MessagepackDriver with default serializer."""
        if "store" in kwargs:
            raise ValueError("Store of MessagepackDriver is fixed, `store` cannot be provided.")
        super().__init__(url=url, serializer=MessagepackSerializer(), **kwargs)
