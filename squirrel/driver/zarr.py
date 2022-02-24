from __future__ import annotations

import typing as t

from squirrel.driver.driver import MapDriver

if t.TYPE_CHECKING:
    from squirrel.iterstream import Composable
    from squirrel.zarr.group import SquirrelGroup

__all__ = ["ZarrDriver"]


def fetch(store: t.MutableMapping, key: str) -> t.Any:
    """An example of a function to fetch data from a zarr store, given the key"""
    return store[key]


class ZarrDriver(MapDriver):
    name = "zarr"

    def __init__(self, url: str, **kwargs):
        """Initializes ZarrDriver."""
        self.url = url

    def get_iter(
        self,
        fetcher_func: t.Callable[[t.MutableMapping, str], t.Any] = fetch,
        storage_options: t.Optional[t.Dict] = None,
        flatten: bool = True,
        **kwargs,
    ) -> Composable:
        """Returns an iterable of samples as specified by `fetcher_func`.

        Args:
            fetcher_func: A function with two arguments, store of type MutableMapping (i.e. a zarr.hierarchy.Group) and
                a key of type string. This function is used to fetch required fields and attributes of a sample.
                Defaults to :py:func:`squirrel.driver.zarr.fetch`.
            storage_options: Keyword arguments passed to :py:func:`squirrel.zarr.convenience.get_group`, which will
            be called to retrieve the store that will be provided to `fetcher_func`.
            flatten: Whether to flatten the returned iterable. Defaults to True.
            **kwargs: Keyword arguments that will be passed to :py:meth:`MapDriver.get_iter`.

        Returns:
            (Composable) Iterable over the items in the store.
        """
        get_kwargs = {} if storage_options is None else storage_options
        get_kwargs["fetcher_func"] = fetcher_func
        return super().get_iter(get_kwargs=get_kwargs, flatten=flatten, **kwargs)

    def get_store(self, mode: str = "r", **storage_options) -> SquirrelGroup:
        """Returns the root zarr group, i.e. zarr group at `self.url`.

        Args:
            mode (str): IO mode (e.g. "r", "w", "a"). Defaults to "r". `mode` affects the store of the returned group.
                See :py:func:`squirrel.zarr.convenience.get_group` for more information.
            **storage_options: Keyword arguments passed to :py:func:`squirrel.zarr.convenience.get_group`.
        """
        from squirrel.zarr.convenience import get_group

        return get_group(path=self.url, mode=mode, **storage_options)

    def keys(self) -> t.Iterator[str]:
        """Returns the keys of the root zarr group."""
        yield from self.get_store().keys()

    def get(self, key: str, fetcher_func: t.Callable, **storage_options) -> t.Iterator:
        """Given `key`, returns a sample defined by `self.fetcher_func`."""
        yield fetcher_func(self.get_store(mode="r", **storage_options), key)
