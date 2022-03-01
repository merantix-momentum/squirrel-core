from __future__ import annotations

import io
import json
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    KeysView,
    List,
    MutableMapping,
    NamedTuple,
    Tuple,
    Type,
    Union,
)

import fsspec

from squirrel.catalog.source import Source
from squirrel.catalog.yaml import catalog2yamlcatalog, prep_yaml, yamlcatalog2catalog
from squirrel.fsspec.fs import get_fs_from_url

if TYPE_CHECKING:
    from ruamel.yaml import Constructor, Representer, SequenceNode

    from squirrel.driver import Driver

__all__ = ["Catalog", "CatalogKey"]


class CatalogKey(NamedTuple):
    """Defines a key in a catalog consisting of the identifier and the version of a source."""

    identifier: str
    version: int = -1

    @classmethod
    def to_yaml(cls, representer: Representer, obj: CatalogKey) -> SequenceNode:
        """Serializes object to SequenceNode."""
        return representer.represent_sequence("!CatalogKey", obj)

    @classmethod
    def from_yaml(cls, constructor: Constructor, node: SequenceNode) -> CatalogKey:
        """Deserializes object from SequenceNode."""
        return CatalogKey(*constructor.construct_sequence(node))


class Catalog(MutableMapping):
    def __init__(self) -> None:
        """Init a Catalog object."""
        # stores {identifier: {version: source_obj}}
        self._sources: Dict[str, Dict[int, CatalogSource]] = {}

    def __repr__(self) -> str:  # noqa D105
        return str({iden: v for iden, vers in self._sources.items() for v in vers})

    def __eq__(self, other: Any) -> bool:  # noqa D105
        if not isinstance(other, Catalog):
            return False

        if len(self.difference(other)) > 0:
            return False

        # deep equal
        return all(source == other[src_id, source.version] for src_id, source in self)

    def __contains__(self, identifier: Union[str, CatalogKey, Tuple[str, int]]) -> bool:  # noqa D105
        if isinstance(identifier, str):
            return identifier in self.sources

        identifier, version = identifier
        return identifier in self and version in self.get_versions(identifier)

    def __delitem__(self, identifier: Union[str, CatalogKey, Tuple[str, int]]) -> None:  # noqa D105
        if isinstance(identifier, str):
            # if not given a specific version, we remove all versions of the identifier
            del self._sources[identifier]
        else:
            identifier, version = identifier
            del self._sources[identifier][version]
            if len(self._sources[identifier]) == 0:
                del self._sources[identifier]

    def __setitem__(self, identifier: Union[str, CatalogKey, Tuple[str, int]], value: Source) -> None:  # noqa D105
        if isinstance(identifier, str):
            version = 1 if identifier not in self else self._handle_latest(identifier, -1) + 1
        else:
            identifier, version = identifier
        versions = self._sources.setdefault(identifier, {})
        versions[version] = CatalogSource(source=value, identifier=identifier, catalog=self, version=version)

    def _handle_latest(self, identifier: str, index: int) -> int:
        return max(self._sources[identifier].keys()) if index == -1 else index

    def __getitem__(self, identifier: Union[str, CatalogKey, Tuple[str, int]]) -> CatalogSource:  # noqa D105
        if isinstance(identifier, str):
            version = -1
        else:
            identifier, version = identifier
        version = self._handle_latest(identifier, version)
        return self._sources[identifier][version]

    def items(self) -> Iterator[Tuple[str, CatalogSource]]:  # noqa D105
        return self.__iter__()

    def __iter__(self) -> Iterator[Tuple[str, CatalogSource]]:  # noqa D105
        return ((iden, source) for iden, versions in self.sources.items() for source in versions.values())

    def keys(self) -> KeysView[str]:  # noqa D105
        return self.sources.keys()

    def __len__(self) -> int:  # noqa D105
        return len(self.keys())

    def copy(self) -> Catalog:
        """Return a deep copy of catalog"""
        # To be 100% save, serialize to string and back
        from squirrel.catalog.yaml import catalog2yamlcatalog, prep_yaml, yamlcatalog2catalog

        yaml = prep_yaml()
        ret = None

        with io.StringIO() as fh:
            yaml.dump(catalog2yamlcatalog(self), fh)
            fh.seek(0)
            ret = yamlcatalog2catalog(yaml.load(fh.read()))
        return ret

    def slice(self, keys: List[str]) -> Catalog:
        """Return a deep copy of catalog that only includes sources with the specified keys."""
        cat_cp = self.copy()
        cat = Catalog()
        for k in keys:
            for version, source in cat_cp.sources[k].items():
                cat[k, version] = source
        return cat

    def join(self, other: Catalog) -> Catalog:
        """Return a joined Catalog out of two disjoint Catalogs."""
        assert len(self.intersection(other)) == 0
        return self.update(other)

    def difference(self, other: Catalog) -> Catalog:
        """Return a Catalog which consists of the difference of the input Catalogs."""
        cat1 = self.copy()
        cat2 = other.copy()

        new_cat = Catalog()
        for a_cat1, a_cat_2 in [(cat1, cat2), (cat2, cat1)]:
            for iden, source in a_cat1.items():
                if iden not in a_cat_2 or source.version not in a_cat_2.sources[iden]:
                    new_cat[iden, source.version] = source
        return new_cat

    def update(self, other: Catalog) -> Catalog:
        """Update the catalog with the sources from other, overwriting existing sources."""
        cat = self.copy()
        oth_cat = other.copy()

        for iden, source in oth_cat.items():
            cat[iden, source.version] = source
        return cat

    def intersection(self, other: Catalog) -> Catalog:
        """Return a Catalog which consists of the intersection of the input Catalogs."""
        cat = self.copy()
        oth_cat = other.copy()

        new_cat = Catalog()
        for iden, source in cat.items():
            ver = source.version
            if (key := (iden, ver)) in oth_cat:
                assert cat[key] == oth_cat[key]
                new_cat[key] = source
        return new_cat

    def filter(self: Catalog, predicate: Callable[[CatalogSource], bool]) -> Catalog:
        """Filter catalog sources based on a predicate."""
        cat = self.copy()

        new_cat = Catalog()
        for iden, source in cat.items():
            if predicate(source):
                new_cat[iden, source.version] = source
        return new_cat

    @staticmethod
    def from_plugins() -> Catalog:
        """Returns a Catalog containing sources specified by plugins."""
        from squirrel.framework.plugins.plugin_manager import squirrel_plugin_manager

        ret = Catalog()
        plugins: List[List[Tuple[CatalogKey, Source]]] = squirrel_plugin_manager.hook.squirrel_sources()
        for plugin in plugins:
            for s_key, source in plugin:
                ret[s_key.identifier, s_key.version] = source

        return ret

    @staticmethod
    def from_dirs(paths: List[str]) -> Catalog:
        """Create a Catalog based on a list of folders containing yaml files."""
        files = []
        for path in paths:
            fs = get_fs_from_url(path)
            a_files = [f for f in fs.ls(path) if f.endswith(".yaml")]
            files += a_files
        return Catalog.from_files(files)

    @staticmethod
    def from_files(paths: List[str]) -> Catalog:
        """Create a Catalog based on a list of paths to yaml files."""
        cat = Catalog()
        for file in paths:
            with fsspec.open(file) as fh:
                new_cat = Catalog.from_str(fh.read())
                cat = cat.join(new_cat)
        return cat

    @staticmethod
    def from_str(cat: str) -> Catalog:
        """Create a Catalog based on a yaml string."""
        yaml = prep_yaml()
        return yamlcatalog2catalog(yaml.load(cat))

    def to_file(self, path: str) -> None:
        """Save a Catalog to a yaml file at the specified path."""
        yaml = prep_yaml()
        with fsspec.open(path, mode="w+") as fh:
            ser = catalog2yamlcatalog(self)
            yaml.dump(ser, fh)

    @property
    def sources(self) -> Dict[str, Dict[int, CatalogSource]]:
        """Sources in the catalog."""
        return self._sources

    def get_versions(self, identifier: str) -> Dict[int, CatalogSource]:
        """Returns versions dictionary given a source identifier."""
        return self.sources.get(identifier, {})


class CatalogSource(Source):
    """Represents a specific version of a source in catalog."""

    def __init__(
        self,
        source: Source,
        identifier: str,
        catalog: Catalog,
        version: int = 1,
    ) -> None:
        """Initialize CatalogSource using a Source."""
        super().__init__(driver_name=source.driver_name, driver_kwargs=source.driver_kwargs, metadata=source.metadata)
        self._identifier = identifier
        self._version = version
        self._catalog = catalog

    def __eq__(self, other: Any) -> bool:  # noqa D105
        if not isinstance(other, CatalogSource):
            return False
        if self.identifier != other.identifier:
            return False
        if self.version != other.version:
            return False
        return super().__eq__(other)

    def __repr__(self) -> str:  # noqa D105
        vars = ("identifier", "driver_name", "driver_kwargs", "metadata", "version")
        dct = {k: getattr(self, k) for k in vars}
        return json.dumps(dct, indent=2, default=str)

    @property
    def identifier(self) -> str:
        """Identifier of the source, read-only."""
        return self._identifier

    @property
    def version(self) -> int:
        """Version of the source, read-only."""
        return self._version

    @property
    def catalog(self) -> Catalog:
        """Catalog containing the source, read-only."""
        return self._catalog

    def get_driver(self, **kwargs) -> Driver:
        """Returns an instance of the driver specified by the source."""
        from squirrel.framework.plugins.plugin_manager import squirrel_plugin_manager

        plugins: List[List[Type[Driver]]] = squirrel_plugin_manager.hook.squirrel_drivers()
        for plugin in plugins:
            for driver_cls in plugin:
                if driver_cls.name == self.driver_name:
                    return driver_cls(catalog=self.catalog, **{**self.driver_kwargs, **kwargs})

        raise ValueError(f"Driver {self.driver_name} not found.")
