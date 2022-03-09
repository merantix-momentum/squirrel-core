from __future__ import annotations

import typing as t

from squirrel.driver.store_driver import StoreDriver
from squirrel.iterstream import Composable
from squirrel.serialization import JsonSerializer
from squirrel.store import SquirrelStore

__all__ = ["JsonlDriver"]


class JsonlDriver(StoreDriver):
    """A StoreDriver that by default uses SquirrelStore with jsonl serialization."""

    name = "jsonl"

    def __init__(self, url: str, deser_hook: t.Optional[t.Callable] = None, **kwargs):
        """Initializes JsonlDriver with default store and serializer.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            deser_hook (Callable): Callable that is passed as `object_hook` to :py:class:`JsonDecoder` during json
                deserialization. Defaults to None.
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        if "store" in kwargs:
            raise ValueError("Store of JsonlDriver is fixed, `store` cannot be provided.")
        super().__init__(store=SquirrelStore(url=url, serializer=JsonSerializer(deser_hook=deser_hook)), **kwargs)

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
