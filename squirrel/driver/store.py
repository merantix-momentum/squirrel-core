from __future__ import annotations

from typing import Any, Iterable, TYPE_CHECKING

from squirrel.driver.driver import MapDriver
from squirrel.serialization import SquirrelSerializer
from squirrel.store import SquirrelStore
from squirrel.store.squirrel_store import CacheStore

if TYPE_CHECKING:
    from squirrel.iterstream import Composable
    from squirrel.store.store import AbstractStore


class StoreDriver(MapDriver):
    """A :py:class`MapDriver` implementation, which uses an :py:class`AbstractStore` instance to retrieve its items.

    The store used by the driver can be accessed via the :py:property:`store` property.
    """

    name = "store_driver"

    def __init__(
        self,
        url: str,
        serializer: SquirrelSerializer,
        cache_url: str | None = None,
        storage_options: dict[str, Any] | None = None,
        cash_storage_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Initializes StoreDriver.

        Args:
            url (str): the url of the store
            serializer (SquirrelSerializer): serializer to be passed to SquirrelStore
            cache_url (str): if provided, the data will be cached in a store at this url
            storage_options (Optional[Dict[str, Any]]): a dict with keyword arguments to be passed to store initializer
            cash_storage_options (Optional[Dict[str, Any]]): a dict with keyword arguments to be passed to
                cache store initializer
            **kwargs: Keyword arguments to pass to the super class initializer.
        """
        super().__init__(**kwargs)
        self.url = url
        self.serializer = serializer
        self.storage_options = storage_options if storage_options is not None else {}
        self.cash_storage_options = cash_storage_options if cash_storage_options is not None else {}
        self.cache_url = cache_url
        self._store = None

    def get_iter(self, flatten: bool = True, **kwargs) -> Composable:
        """Returns an iterable of items in the form of a :py:class:`squirrel.iterstream.Composable`, which allows
        various stream manipulation functionalities.

        Items are fetched using the :py:meth:`get` method. The returned :py:class:`Composable` iterates over the items
        in the order of the keys returned by the :py:meth:`keys` method.

        Args:
            flatten (bool): Whether to flatten the returned iterable. Defaults to True.
            **kwargs: Other keyword arguments passed to `super().get_iter()`. For details, see
                :py:meth:`squirrel.driver.MapDriver.get_iter`.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the items in the store.
        """
        return super().get_iter(flatten=flatten, **kwargs)

    def get(self, key: Any, **kwargs) -> Iterable:
        """Returns an iterable over the items corresponding to `key` using the store instance.

        Calls and returns the result of :py:meth:`self.store.get`. Subclasses might filter or manipulate the iterable
        over items returned from the store.

        Args:
            key (Any): Key with which the items will be retrieved. Must be of type and format that is supported by the
            store instance.
            **kwargs: Keyword arguments passed to the :py:meth:`self.store.get` method.

        Returns:
            (Iterable) Iterable over the items corresponding to `key`, as returned from the store.
        """
        return self.store.get(key, **kwargs)

    def keys(self, **kwargs) -> Iterable:
        """Returns an iterable over all keys to the items that are obtainable through the driver.

        Calls and returns the result of :py:meth:`self.store.keys`. Subclasses might filter or manipulate the iterable
        over keys returned from the store.

        Args:
            **kwargs: Keyword arguments passed to the :py:meth:`self.store.keys` method.

        Returns:
            (Iterable) Iterable over all keys in the store, as returned from the store.
        """
        return self.store.keys(**kwargs)

    @property
    def store(self) -> AbstractStore:
        """Store that is used by the driver."""
        if self._store is None:
            if self.cache_url is None:
                self._store = SquirrelStore(url=self.url, serializer=self.serializer, **self.storage_options)
            else:
                self._store = CacheStore(
                    url=self.url, serializer=self.serializer, cache_url=self.cache_url, **self.cash_storage_options
                )
        return self._store
