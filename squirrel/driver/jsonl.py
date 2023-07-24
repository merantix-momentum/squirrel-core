from __future__ import annotations

import typing as t

from squirrel.driver.store import StoreDriver
from squirrel.iterstream import Composable
from squirrel.serialization import JsonSerializer

__all__ = ["JsonlDriver"]


class JsonlDriver(StoreDriver):
    """
    A StoreDriver that by default uses SquirrelStore with jsonl serialization.  Please see the parent class for
    additional configuration
    """

    name = "jsonl"

    def __init__(
        self,
        url: str,
        deser_hook: t.Optional[t.Callable] = None,
        storage_options: dict[str, t.Any] | None = None,
        **kwargs,
    ):
        """Initializes JsonlDriver with default serializer.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            deser_hook (Callable): Callable that is passed as `object_hook` to :py:class:`JsonDecoder` during json
                deserialization. Defaults to None.
            storage_options (Dict): a dictionary containing storage_options to be passed to fsspec.
                Example of storage_options if you want to enable `fsspec` caching:
                `storage_options={"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}`
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        if "store" in kwargs:
            raise ValueError("Store of JsonlDriver is fixed, `store` cannot be provided.")
        super().__init__(
            url=url, serializer=JsonSerializer(deser_hook=deser_hook), storage_options=storage_options, **kwargs
        )

    def get_iter(
        self,
        get_kwargs: t.Optional[t.Dict] = None,
        **kwargs,
    ) -> Composable:
        """Returns an iterable of samples as specified by `fetcher_func`.

        Args:
            get_kwargs (Dict): Keyword arguments that will be passed as `get_kwargs` to :py:meth:`MapDriver.get_iter`.
                `get_kwargs` will always have `compression="gzip"`. Defaults to None.
            **kwargs: Other keyword arguments that will be passed to :py:meth:`MapDriver.get_iter`.

        Returns:
            (Composable) Iterable over the items in the store.
        """
        if get_kwargs is None:
            get_kwargs = {}
        get_kwargs["compression"] = "gzip"
        return super().get_iter(get_kwargs=get_kwargs, **kwargs)
