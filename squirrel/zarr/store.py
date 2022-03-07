"""
At a low level, squirrel offers some store options that could enhance or modify zarr stores such as

- reducing number of HTTP calls zarr has to make in order to fetch items from cloud datasets
- fetching items from datasets in an asynchronous manner
- caching storage path keys and metadata

to allow fast cluster / full iteration through the dataset.

Currently, we offer the following store:

- :py:class:`~squirrel.zarr.store.SquirrelFSStore` is based on ``zarr.storage.FSStore``. In SquirrelFSStore, we use the
  :py:mod:`squirrel.fsspec` module to control HTTP connections to cloud buckets.
"""

import logging
import re
import typing as t
from collections.abc import MutableMapping
from threading import Lock

import msgpack
from fsspec.spec import AbstractFileSystem
from numcodecs import Blosc
from numcodecs.abc import Codec
from zarr.errors import FSPathExistNotDir, ReadOnlyError
from zarr.storage import FSStore, getsize
from zarr.util import json_dumps, json_loads

from squirrel import __version__ as sq__version__
from squirrel.constants import SQUIRREL_PREFIX, URL
from squirrel.dataset.stream_dataset import StreamDataset
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.zarr.key import (
    KeyFetcher,
    is_dir,
    is_squirrel_key,
    is_zarr_chunk,
    is_zarr_key,
    normalize_key,
    retrieve_all_keys,
)

logger = logging.getLogger(__name__)


def suggest_compression() -> Codec:
    """Suggested compression method from squirrel."""
    return Blosc(cname="lz4hc", clevel=5, shuffle=Blosc.BITSHUFFLE)


def is_cached(store: MutableMapping) -> bool:
    """Returns True if store is cached."""
    return isinstance(store, CachedStore) and CachedStore.CPREFIX + CachedStore.CKEY in store.store


def copy_store(src: MutableMapping, dst: MutableMapping, keys: t.List[str] = None) -> None:
    """Copy keys from one store to another.

    Args:
        src (MutableMapping): Source store.
        dst (MutableMapping): Destination store.
        keys (List[str], optional): Keys to copy. If None, all keys of the source store will be copied. Defaults to
            None.
    """
    if keys is None:
        keys = src.keys()
    for k in keys:
        dst[k] = src[k]
    try:
        # some stores have to be flushed to persist state
        dst.flush()
    except AttributeError:
        pass


def write_routing_to_store(store: MutableMapping, idx: int, shard: t.List[str], compress: bool = True) -> None:
    """Write a shard routing based on a list of keys defined in shard."""
    routing = {}

    logger.debug(f"create shard {idx} routing")
    routing = {k: (idx, shard_items) for shard_items, k in enumerate(sorted(shard))}

    routing = json_dumps(routing)
    if compress:
        logger.info(f"compress routing information of shard {idx}")
        routing = suggest_compression().encode(routing)

    logger.info(f"store routing information of shard {idx}")
    store[CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}" + CachedStore.CROUTING] = routing
    logger.debug(f"stored routing information of shard {idx}")


def write_shard_to_store(
    store: MutableMapping,
    idx: int,
    target_store: MutableMapping = None,
    compress: bool = True,
) -> None:
    """Write a shard based on the existing routing created by :py:meth:`write_routing_to_store`.

    Args:
        store (MutableMapping): Source store that contains the routing table.
        idx (int): Shard index.
        target_store (MutableMapping, optional): Store to which the shard will be written. If None, shard will be
            written to `store`. Defaults to None.
        compress (bool, optional): Whether to compress the shard. Defaults to True.
    """
    if target_store is None:
        target_store = store

    # retrieve routing table
    routing = store[CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}" + CachedStore.CROUTING]
    try:
        # try to decompress the routing. if it fails, fallback to non-compressed
        routing = suggest_compression().decode(routing)
    except RuntimeError:
        pass
    routing = json_loads(routing).keys()

    logger.debug(f"create shard {idx}")
    # assume that the writing machine has at least 1/8 of the memory of the reading machine
    cache_size = int(len(routing) / 8)
    fetcher = KeyFetcher(store=store)
    with StreamDataset(fetcher=fetcher, cache_size=cache_size) as sd:
        for k in routing:
            sd.request(k)
        shard_items = [sd.retrieve() for _ in routing]

    logger.debug(f"pack shard {idx}")
    shard_enc = msgpack.packb(shard_items, use_bin_type=True)

    if compress:
        logger.debug("compress shard")
        shard_enc = suggest_compression().encode(shard_enc)

    logger.debug(f"store shard {idx}")
    target_store[CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}"] = shard_enc
    logger.debug(f"stored shard {idx}")


def cache_sharded_store(
    store: MutableMapping, shards: t.List[t.List[str]], compress: bool = True, target_store: MutableMapping = None
) -> None:
    """Cache a sharded store."""
    if target_store is None:
        target_store = store

    logger.debug(f"write {len(shards)} shard routings in parallel")
    for idx, shard in enumerate(shards):
        write_routing_to_store(target_store, idx, shard, compress=compress)
    logger.debug("shard routing creation finished")

    logger.debug(f"write {len(shards)} shards in parallel")
    for idx, _ in enumerate(shards):
        write_shard_to_store(store, idx, target_store=target_store, compress=compress)
    logger.debug("shard creation finished")


def remove_uncached_store(store: MutableMapping) -> None:
    """Delete uncached data from a store. Can be reversed by :py:meth:`restore_uncached_store`."""
    store_r = CachedStore(store)
    if store_r._crouting is not None and store_r._ckeys is not None:
        logger.debug("found squirrel keys in store. will remove them")
        for k in store.keys():
            if not is_squirrel_key(k) and not k.endswith(".zarray") and not k.endswith(".zgroup"):
                del store[k]


def restore_uncached_store(store: MutableMapping) -> None:
    """Restore a store from cached data. Reverse of :py:meth:`remove_uncached_store`."""
    store_r = CachedStore(store)
    copy_store(store_r, store)


def clean_cached_store(store: MutableMapping) -> None:
    """Remove all squirrel keys (including cached data and shards) from the store."""

    if CachedStore.CPREFIX + CachedStore.CKEY in store:
        logging.debug("delete cached data")
        del store[CachedStore.CPREFIX + CachedStore.CKEY]

    logging.debug("delete shards if present")
    clean_sharded_store(store)


def clean_sharded_store(store: MutableMapping) -> None:
    """Remove all squirrel shards from the store."""

    logging.debug("delete shards if present")
    idx = 0
    while True:
        a_r_key = CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}" + CachedStore.CROUTING
        a_s_key = CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}"
        if a_r_key not in store and a_s_key not in store:
            break

        logging.debug(f"delete shard {idx}")
        if a_r_key in store:
            del store[a_r_key]
        if a_s_key in store:
            del store[a_s_key]
        idx += 1


def get_store_shard_keys(store: MutableMapping) -> t.List[t.Tuple[str, str]]:
    """Retrieve all shard routing and shard keys of a store."""

    # a CachedStore does not expose squirrel keys -> get the underlying store
    if isinstance(store, CachedStore):
        store = store.store

    ret = []
    idx = 0
    while True:
        a_r_key = CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}" + CachedStore.CROUTING
        a_s_key = CachedStore.CPREFIX + CachedStore.CSHARD + f"_{idx}"
        if a_r_key in store and a_s_key in store:
            ret.append((a_r_key, a_s_key))
            idx += 1
        else:
            break
    return ret


def cache_meta_store(
    store: MutableMapping,
    cache_keys: bool = True,
    cache_meta: bool = False,
    compress: bool = True,
    clean: bool = False,
    target_store: MutableMapping = None,
) -> None:
    """
    Cache the store keys (item names), dirs (absolute paths), len (number of items) and meta (zarr metadata),
    and dump them into a json file. Cleaning beforehand or compressing afterwards is optional.

    Args:
        store (MutableMapping): Any given store that is a child class of MutableMapping.
        cache_keys (bool, optional): If True, cache the keys for the store. Defaults to True.
        cache_meta (bool, optional): If True, cache the meta for the store. The 'meta' here refers only to zarr
            metadata stored in '.zgroup' and '.zattrs' and '.zarray' (see :py:meth:`squirrel.zarr.key.is_zarr_key`).
            Defaults to False.
        compress (bool, optional): If True, will compress the store chunk data using squirrel recommended compression
            methods. Defaults to True.
        clean (bool, optional): If True, clean all previous cache content inside the store. Defaults to False.
        target_store (MutableMapping, optional): Store where the cached data is written to. If None, `store` will be
            used. Defaults to None.
    """
    if target_store is None:
        target_store = store

    if clean:
        # clear cached
        clean_cached_store(target_store)

    # init ckey
    logger.debug("init cached data")
    target_store[CachedStore.CPREFIX + CachedStore.CKEY] = json_dumps({})

    # gather cached data
    cache = {"squirrel_version": sq__version__}

    if cache_keys:
        logger.debug("cache keys")
        keys = [k for k in retrieve_all_keys(store) if not is_squirrel_key(k)]
        cache["keys"] = keys

        logger.debug("cache len")
        cache["len"] = len(keys)

        def _fetch_dirs(k: str, f_keys: t.Union[t.Set[str], t.List[str]]) -> t.Dict[str, t.List[str]]:
            k = normalize_key(k)
            a_dirs = {normalize_key(d[len(k) :]).split("/")[0] for d in f_keys if d.startswith(k) or k == ""}
            a_dirs = {
                d for d in a_dirs if not is_squirrel_key(d) and d != "" and not is_zarr_chunk(d) and not is_zarr_key(d)
            }

            res = {k: sorted(list(a_dirs))}
            for a_dir in a_dirs:
                new_f_keys = {f for f in f_keys if f.startswith(k + a_dir)}
                res = {**res, **_fetch_dirs(k + a_dir, new_f_keys)}

            return res

        logger.debug("cache dirs")
        cache["dirs"] = _fetch_dirs("", keys)

    if cache_meta:
        logger.debug("cache meta")
        cache["meta"] = {key: json_loads(store[key]) for key in store if is_zarr_key(key)}

    # write cached
    cache = json_dumps(cache)

    if compress:
        logger.debug("compress cached data")
        cache = suggest_compression().encode(cache)

    logger.debug("write cached data")
    target_store[CachedStore.CPREFIX + CachedStore.CKEY] = cache


class CachedStore(MutableMapping):
    """
    Loads cached info about a store into memory, acts as an interface between the underlying store and any data-loading
    logic that needs to iterate over the store content and metadata. Allows fast iteration over the whole store
    contents. If no cache is available, it falls back to the underlying store.
    """

    CPREFIX = SQUIRREL_PREFIX
    CKEY = "zcached"
    CSHARD = "zshard"
    CROUTING = "_routing"

    def __init__(self, store: MutableMapping) -> None:
        """Init the store, load cached keys and meta from the json file.

        Args:
            store (MutableMapping): Any store that is a child class of MutableMapping.
        """
        self.store = store
        self.url = self.store.url

        self._ckeys = self._clen = self._cdirs = self._cmeta = self._crouting = None

        # if cached json file already exists, load it into memory.
        if self.CPREFIX + self.CKEY in store:
            cache = store[self.CPREFIX + self.CKEY]
            try:
                # try to decompress the metadata. if it fails, fallback to non-compressed
                cache = suggest_compression().decode(cache)
                logger.debug("use compressed cached data")
            except RuntimeError:
                logger.debug("could not decompress cached data, use it uncompressed")
                pass
            cache = json_loads(cache)
            if "keys" in cache:
                self._ckeys = set(cache["keys"])
                logger.debug("use cached keys")
            if "dirs" in cache:
                self._cdirs = cache["dirs"]
                logger.debug("use cached dirs")
            if "len" in cache:
                self._clen = cache["len"]
                logger.debug("use cached len")
            if "meta" in cache:
                self._cmeta = cache["meta"]
                logger.debug("use cached meta data")
        else:
            logger.warning("The provided store is not cached. Will fallback to the uncached store.")

        # if shards already exist, loads the routing (a mapper from item key to its real path in shards) file into
        # memory.
        self._crouting = self._routing()
        if self._crouting is not None:
            self._ashard_id = None
            self._ashard = None
            self._switchshard_mutex = Lock()
        else:
            logger.warning("The provided store is not sharded. Will fallback to the un-sharded store.")

    @property
    def fs(self) -> AbstractFileSystem:
        """File system of the store."""
        return self.store.fs

    @property
    def path(self) -> str:
        """Path of the store."""
        return self.store.path

    def _routing(self) -> t.Optional[t.Dict]:
        """Helper that returns the routing if it exists, else None."""
        routing = None
        sc = suggest_compression()

        for route_k, _ in get_store_shard_keys(self):
            a_routing = self.store[route_k]
            try:
                # try to decompress the routing. if it fails, fallback to non-compressed
                a_routing = sc.decode(a_routing)
            except RuntimeError:
                pass
            a_routing = json_loads(a_routing)

            if routing is None:
                routing = {}
            routing = {**routing, **a_routing}
        return routing

    def __getitem__(self, key: str) -> t.Any:
        """Get data according to key.

        Fetches an item with a zarr key plainly and ignores items with squirrel keys (as they should, since they are
        protected from end users) and fetch items within shards with the aid of shard routing `self._crouting`, which
        provides a mapping between keys and a tuple (shard_id, idx) leads to items in shard.

        Args:
            key (str): Relative path to the target file.

        Returns:
            Any: Item corresponding to the key.
        """
        key = normalize_key(key)
        if self._cmeta is not None and key in self._cmeta:
            return self._cmeta[key]
        elif self._crouting is not None and not is_squirrel_key(key) and key in self._crouting:
            shard_id, idx = self._crouting[key]
            # prevent that multiple threads read from a shard while a shard is getting switched
            with self._switchshard_mutex:
                if self._ashard_id != shard_id:
                    logger.debug(f"switch from shard {self._ashard_id} to {shard_id}")
                    # switch shard if we are currently in another shard outside of the key location.
                    shard_key = self.CPREFIX + self.CSHARD + f"_{shard_id}"
                    ashard = self.store[shard_key]

                    # try decompressing the shard
                    sc = suggest_compression()
                    try:
                        # try to decompress the shard. if it fails, fallback to non-compressed
                        ashard = sc.decode(ashard)
                    except RuntimeError:
                        pass

                    self._ashard = msgpack.unpackb(ashard, raw=False)
                    self._ashard_id = shard_id
                return self._ashard[idx]
        else:
            return self.store[key]

    def __contains__(self, key: str) -> bool:
        """Check if key exists in the cached store."""
        key = normalize_key(key)
        if self._ckeys is not None:
            return key in self._ckeys
        else:
            return key in self.store

    def keys(self) -> t.Generator[str, None, None]:
        """Get all keys from the store."""
        if self._ckeys is not None:
            for key in self._ckeys:
                yield key
        else:
            for key in retrieve_all_keys(self.store):
                yield key

    def __iter__(self) -> t.Iterator[str]:
        """Iterator over all store items."""
        return iter(self.keys())

    def __len__(self) -> int:
        """Total number of keys inside the cached store."""
        if self._clen is not None:
            return self._clen
        else:
            return len(self.store)

    def __delitem__(self, key: str) -> None:
        """
        Remove an item according to key. Since CachedStore is read only, will raise an ReadOnlyError
        if called.
        """
        raise ReadOnlyError()

    def __setitem__(self, key: str, value: t.Any) -> None:
        """
        Set an item according to key, value pair. Since CachedStore is read only, will raise an
        ReadOnlyError if called.
        """
        raise ReadOnlyError()

    def getsize(self, path: t.Union[str, bytes, None] = None) -> int:
        """Get the total size underlying a particular path inside the store."""
        return getsize(self.store, path)

    def listdir(self, path: str = "") -> t.List[str]:
        """List all sub-directories under a given path."""
        if self._cdirs is not None:
            return self._cdirs[normalize_key(path)]
        else:
            return self.store.listdir(path)

    def array_cluster(self, pattern: str = None) -> t.List[t.Set[str]]:
        """Returns cluster of groups in this store that can be efficiently read in bulk.

        Args:
            pattern (str, optional): Regex search pattern that needs to be included in the fetched array cluster.
                Defaults to None.

        Returns:
            List[Set[str]]: List of sets of array cluster keys that can be iterated over fast together.
        """

        if pattern is not None:
            pattern = re.compile(pattern)

        groups = {}
        for k in self.keys():
            if k.endswith(".zarray"):
                k_wo_zarray = k.replace(".zarray", "")
                if pattern is not None and not pattern.search(k_wo_zarray):
                    continue

                if self._crouting is None:
                    shard_id = 0
                else:
                    shard_id, _ = self._crouting[k]

                if shard_id not in groups:
                    groups[shard_id] = set()
                groups[shard_id].add(k_wo_zarray)
        return list(groups.values())


class SquirrelFSStore(FSStore):
    """Alters the default fs argument to :py:class:`~squirrel.fsspec.custom_gcsfs.CustomGCSFileSystem` if the url
    location directs to google cloud bucket. This allows us to use the custom gcsfs to open FSStore inside Zarr. The
    main purpose is too add google 400 error into retriable options when doing https requests to google cloud storage
    buckets so that 400 does not break our cloud build, tests, and other time consuming operations. For the main issue,
    see https://github.com/dask/gcsfs/issues/290.
    """

    def __init__(
        self,
        url: URL,
        key_separator: str = ".",
        mode: str = "w",
        exceptions: t.Sequence[t.Type[Exception]] = (KeyError, PermissionError, IOError),
        check_exists: bool = False,
        **storage_options,
    ) -> None:
        """Initialize SquirrelFSStore.

        Args:
            url (URL): Path to the store.
            key_separator (str, optional): Separator placed between the dimensions of a chunk. Defaults to ".".
            mode (str, optional): File IO mode to use. Defaults to "w".
            exceptions (Sequence[Type[Exception]], optional): When accessing data, any of these exceptions will be
                treated as a missing key. Defaults to (KeyError, PermissionError, IOError).
            check_exists (bool, optional): Whether to check that `url` corresponds to a directory. Defaults to False.
            **storage_options: Storage options to be passed to fsspec.

        Raises:
            FSPathExistNotDir: If `check_exists == True` and `url` does not correspond to a directory.
        """
        self.url = url
        self.normalize_keys = False
        self.key_separator = key_separator
        self.fs = get_fs_from_url(url, **storage_options)
        self.map = self.fs.get_mapper(url)
        self.path = self.fs._strip_protocol(url)
        self.mode = mode
        self.exceptions = exceptions

        if check_exists and self.fs.exists(self.path) and not self.fs.isdir(self.path):
            raise FSPathExistNotDir(url)

    def listdir(self, path: URL = None) -> t.List[str]:
        """List all dirs under the path, except for squirrel keys and zarr keys."""
        dir_path = self.dir_path(path)
        try:
            children = sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in self.fs.ls(dir_path, detail=False))
            return [c for c in children if is_dir(c)]
        except IOError:
            return []

    def getsize(self, path: URL = None) -> int:
        """Get size of a subdir inside the store."""
        store_path = self.dir_path(path)
        return self.fs.du(store_path, total=True, maxdepth=None)
