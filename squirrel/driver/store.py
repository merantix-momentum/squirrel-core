from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from squirrel.driver.driver import MapDriver
from squirrel.iterstream.source import IterableSource
from squirrel.serialization import SquirrelSerializer
from squirrel.serialization.jsonl import JsonSerializer
from squirrel.serialization.msgpack import MessagepackSerializer
from squirrel.serialization.np import NumpySerializer
from squirrel.serialization.parquet import ParquetSerializer
from squirrel.serialization.png import PNGSerializer
from squirrel.store import SquirrelStore

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
        storage_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Initializes StoreDriver.

        Args:
            url (str): the url of the store
            serializer (SquirrelSerializer): serializer to be passed to SquirrelStore
            storage_options (Optional[Dict[str, Any]]): a dict with keyword arguments to be passed to store initializer
                Example of storage_options if you want to enable `fsspec` caching:
                `storage_options={"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}`
            **kwargs: Keyword arguments to pass to the super class initializer.
        """
        super().__init__(**kwargs)
        self.url = url
        self.serializer = serializer
        self.storage_options = storage_options if storage_options is not None else {}
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

    def get_iter_ray(
        self,
        # keys_iterable: Iterable | None = None,
        # shuffle_key_buffer: int = 1,
        # key_hooks: Iterable[Callable | type[Composable] | functools.partial] | None = None,
        # max_workers: int | None = None,
        prefetch_buffer: int = 10,
        shuffle_item_buffer: int = 1,
        # flatten: bool = False,
        # keys_kwargs: dict | None = None,
        # get_kwargs: dict | None = None,
        key_shuffle_kwargs: dict | None = None,
        item_shuffle_kwargs: dict | None = None,
        drop_last: bool = False,
        batch_size: int = 100,
    ) -> None:
        """TODO: if the same api for get_iter can be used for ray, we should go for that."""
        import ray

        item_shuffle_kwargs = {} if item_shuffle_kwargs is None else item_shuffle_kwargs

        ds = ray.data
        if self.serializer == PNGSerializer:
            ds = ds.read_images(self.url)
        elif self.serializer == NumpySerializer:
            ds = ds.read_numpy(self.url)
        elif isinstance(self.serializer(), ParquetSerializer):
            ds = ds.read_parquet(self.url)
        elif self.serializer == MessagepackSerializer or JsonSerializer:
            raise NotImplementedError("msgpack and jsonl drivers not implemented")
        else:
            raise NotImplementedError(f"serializer is {self.serializer}!")

        ds = ds.iter_batches(
            prefetch_batches=prefetch_buffer,
            batch_format="numpy",
            drop_last=drop_last,
            batch_size=batch_size,
            local_shuffle_buffer_size=shuffle_item_buffer,
            local_shuffle_seed=42,
        )

        it = IterableSource(ds)
        if self.serializer == PNGSerializer:
            it = it.map(lambda x: x["image"]).flatten()
        if self.serializer == NumpySerializer:
            it = it.map(lambda x: x["data"]).flatten()
        if self.serializer == ParquetSerializer:
            import pyarrow as pa

            it = it.map(lambda x: pa.RecordBatch.to_pylist(pa.RecordBatch.from_pydict(x))).flatten()
        return it.shuffle(size=shuffle_item_buffer, **item_shuffle_kwargs)

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
            self._store = SquirrelStore(url=self.url, serializer=self.serializer, **self.storage_options)
        return self._store
