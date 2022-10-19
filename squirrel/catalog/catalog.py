from __future__ import annotations

import io
import json
from typing import Any, Callable, Iterable, Iterator, KeysView, MutableMapping, NamedTuple, TYPE_CHECKING

import fsspec

from squirrel.catalog.source import Source
from squirrel.fsspec.fs import get_fs_from_url

if TYPE_CHECKING:
    from ruamel.yaml import Constructor, Representer, SequenceNode

    from squirrel.driver import Driver

__all__ = ["Catalog", "CatalogKey", "CatalogSource"]


class CatalogKey(NamedTuple):
    """Defines a key in a catalog consisting of the identifier and the version of a source.

    A CatalogKey uniquely describes a :py:class:`~squirrel.catalog.Source` in a :py:class:`~squirrel.catalog.Catalog`.
    """

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
        r"""A dictionary-like data structure that versions and maintains :py:class:`~squirrel.catalog.Source`\s.

        Each source in a Catalog is stored with a :py:class:`squirrel.catalog.CatalogKey`, which is unique.

        It is possible to combine multiple Catalogs together or find the difference between them. Refer to class methods
        for more information.

        A Catalog can be [de-]serialized. You can check out the ``from_xxx()`` and ``to_xxx()`` methods for additional
        information.
        """
        self._sources: dict[str, CatalogSource] = {}

    def __repr__(self) -> str:  # noqa D105
        return str(sorted(set(self.sources.keys()), key=lambda x: x.lower()))

    def __eq__(self, other: Any) -> bool:  # noqa D105
        if not isinstance(other, Catalog):
            return False

        if len(self.difference(other)) > 0:
            return False

        # deep equal
        for k in self.keys():
            for v in self[k].versions.keys():
                if self[k][v] != other[k][v]:
                    return False

        return True

    def __contains__(self, identifier: str | CatalogKey | tuple[str, int]) -> bool:  # noqa D105
        if isinstance(identifier, str):
            return identifier in self._sources

        identifier, version = identifier
        return identifier in self._sources and version in self._sources[identifier]

    def __delitem__(self, identifier: str | CatalogKey | tuple[str, int]) -> None:  # noqa D105
        if isinstance(identifier, str):
            # if not given a specific version, we remove all versions of the identifier
            del self._sources[identifier]
        else:
            identifier, version = identifier
            del self._sources[identifier][version]
            if len(self._sources[identifier]) == 0:
                del self._sources[identifier]

    def __setitem__(self, identifier: str | CatalogKey | tuple[str, int], value: Source) -> None:  # noqa D105
        if isinstance(identifier, str):
            version = 1 if identifier not in self else self._sources[identifier][-1].version + 1
        else:
            identifier, version = identifier

        if identifier not in self:
            self._sources[identifier] = CatalogSource(
                source=value, identifier=identifier, catalog=self, version=version
            )
        else:
            self._sources[identifier][version] = value

    def __getitem__(self, identifier: str | CatalogKey | tuple[str, int]) -> CatalogSource:  # noqa D105
        if identifier not in self:
            if isinstance(identifier, str):
                # return a dummy object to let the user set a version directly
                return DummyCatalogSource(identifier, self)
            else:
                # DummyCatalogSource only allows setting the version after initialization, cannot set it at this point
                raise KeyError(f"The catalog does not have an entry for identifier {identifier}.")

        if isinstance(identifier, str):
            # we return the latest if the version is not specified explicitly
            version = -1
        else:
            identifier, version = identifier

        return self.sources[identifier][version]

    def items(self) -> Iterator[tuple[str, Source]]:  # noqa D105
        return self.__iter__()

    def __iter__(self) -> Iterator[tuple[str, Source]]:  # noqa D105
        for k, v in self.sources.items():
            yield k, v[-1]

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

    def slice(self, keys: list[str]) -> Catalog:
        """Return a deep copy of catalog were only by key specified sources get copied."""
        cat_cp = self.copy()
        cat = Catalog()
        for k in keys:
            for v in cat_cp[k].versions:
                cat[k][v] = cat_cp[k][v]
        return cat

    def join(self, other: Catalog) -> Catalog:
        """Return a joined Catalog out of two disjoint Catalogs."""
        assert len(self.intersection(other)) == 0
        return self.union(other)

    def difference(self, other: Catalog) -> Catalog:
        """Return a Catalog which consists of the difference of the input Catalogs."""
        cat1 = self.copy()
        cat2 = other.copy()

        new_cat = Catalog()
        for a_cat1, a_cat_2 in [(cat1, cat2), (cat2, cat1)]:
            for k in a_cat1.keys():
                for ver_id, version in a_cat1[k].versions.items():
                    if k not in a_cat_2 or ver_id not in a_cat_2[k]:
                        new_cat[k][ver_id] = version
        return new_cat

    def union(self, other: Catalog) -> Catalog:
        """Return a Catalog which consists of the union of the input Catalogs."""
        cat = self.copy()
        oth_cp = other.copy()

        for k in oth_cp.keys():
            for ver_id, version in oth_cp[k].versions.items():
                cat[k][ver_id] = version
        return cat

    def intersection(self, other: Catalog) -> Catalog:
        """Return a Catalog which consists of the intersection of the input Catalogs."""
        cat1 = self.copy()
        cat2 = other.copy()

        new_cat = Catalog()
        for k in cat1.keys():
            for ver_id, version in cat1[k].versions.items():
                if k in cat2 and ver_id in cat2[k].versions:
                    assert cat1[k][ver_id] == cat2[k][ver_id]
                    new_cat[k][ver_id] = version
        return new_cat

    def filter(self: Catalog, predicate: Callable[[CatalogSource], bool]) -> Catalog:
        """Filter catalog sources based on a predicate."""
        cat1 = self.copy()

        new_cat = Catalog()
        for k in cat1.keys():
            for ver_id, version in cat1[k].versions.items():
                if predicate(version):
                    new_cat[k][ver_id] = version
        return new_cat

    @staticmethod
    def from_plugins() -> Catalog:
        """Returns a Catalog containing sources specified by plugins."""
        from squirrel.framework.plugins.plugin_manager import squirrel_plugin_manager

        ret = Catalog()
        plugins = squirrel_plugin_manager.hook.squirrel_sources()
        for plugin in plugins:
            for s_key, source in plugin:
                ret[s_key.identifier][s_key.version] = source

        return ret

    @staticmethod
    def from_dirs(paths: list[str]) -> Catalog:
        """Create a Catalog based on a list of folders containing yaml files."""
        files = []
        for path in paths:
            fs = get_fs_from_url(path)
            a_files = [f for f in fs.ls(path) if f.endswith(".yaml")]
            files += a_files
        return Catalog.from_files(files)

    @staticmethod
    def from_files(paths: list[str]) -> Catalog:
        """Create a Catalog based on a list of paths to yaml files."""
        cat = Catalog()
        for file in paths:
            with fsspec.open(file, mode="r") as fh:
                new_cat = Catalog.from_str(fh.read())
                cat = cat.join(new_cat)
        return cat

    @staticmethod
    def from_str(cat: str) -> Catalog:
        """Create a Catalog based on a yaml string."""
        from squirrel.catalog.yaml import prep_yaml, yamlcatalog2catalog

        yaml = prep_yaml()
        return yamlcatalog2catalog(yaml.load(cat))

    def to_file(self, path: str) -> None:
        """Save a Catalog to a yaml file at the specified path."""
        from squirrel.catalog.yaml import catalog2yamlcatalog, prep_yaml

        yaml = prep_yaml()
        with fsspec.open(path, mode="w") as fh:
            ser = catalog2yamlcatalog(self)
            yaml.dump(ser, fh)

    @property
    def sources(self) -> dict[str, CatalogSource]:
        """Read only property"""
        return self._sources


class CatalogSource(Source):
    def __init__(
        self,
        source: Source,
        identifier: str,
        catalog: Catalog,
        version: int = 1,
        versions: dict[int, CatalogSource] | None = None,
    ) -> None:
        """Wraps a Source with an identifier and a version number so that a unique version can be stored in a Catalog.

        Through CatalogSource, it is possible to access other versions of the Source as well as the Catalog storing
        them. Using :py:meth:`get_driver`, the driver that can read from the Source can be obtained.
        """
        self._identifier = identifier
        self._version = version
        self._versions = {version: self} if versions is None else versions
        self._catalog = catalog
        super().__init__(driver_name=source.driver_name, driver_kwargs=source.driver_kwargs, metadata=source.metadata)

    def __eq__(self, other: Any) -> bool:  # noqa D105
        if not isinstance(other, CatalogSource):
            return False
        if self.identifier != other.identifier:
            return False
        if self.driver_kwargs != other.driver_kwargs:
            return False
        if self.version != other.version:
            return False
        if self.metadata != other.metadata:
            return False
        # do not check versions or catalog
        return True

    def __repr__(self) -> str:  # noqa D105
        dct = {
            "identifier": self.identifier,
            "driver_name": self.driver_name,
            "driver_kwargs": self.driver_kwargs,
            "metadata": self.metadata,
            "version": self.version,
            "versions": list(self.versions.keys()),
        }
        return json.dumps(dct, indent=2, default=str)

    def _handle_latest(self, index: int) -> int:
        if index == -1:
            return sorted(self.versions.keys())[-1]
        return index

    def __delitem__(self, index: int) -> None:  # noqa D105
        index = self._handle_latest(index)
        del self._versions[index]

    def __setitem__(self, index: int, value: Source) -> None:  # noqa D105
        assert index > 0
        self._versions[index] = CatalogSource(
            source=value, identifier=self._identifier, catalog=self._catalog, version=index, versions=self.versions
        )

    def __contains__(self, index: int) -> bool:  # noqa D105
        index = self._handle_latest(index)
        return index in self.versions

    def __getitem__(self, index: int) -> CatalogSource:  # noqa D105
        index = self._handle_latest(index)
        return self.versions[index]

    def __iter__(self) -> Iterable[CatalogSource]:  # noqa D105
        return iter(self.versions.values())

    def __len__(self) -> int:  # noqa D105
        return len(self.versions.keys())

    @property
    def identifier(self) -> str:
        """Read only property"""
        return self._identifier

    @property
    def version(self) -> int:
        """Read only property"""
        return self._version

    @property
    def versions(self) -> dict[int, CatalogSource]:
        """Read only property"""
        return self._versions

    def get_driver(self, **kwargs) -> Driver:
        """Returns an instance of the driver specified by the source."""
        from squirrel.framework.plugins.plugin_manager import squirrel_plugin_manager

        plugins: list[list[type[Driver]]] = squirrel_plugin_manager.hook.squirrel_drivers()
        for plugin in plugins:
            for driver_cls in plugin:
                if driver_cls.name == self.driver_name:
                    return driver_cls(catalog=self._catalog, **{**self.driver_kwargs, **kwargs})

        raise ValueError(f"driver {self.driver_name} not found")


class DummyCatalogSource:
    def __init__(
        self,
        identifier: str,
        catalog: Catalog,
    ) -> None:
        """Init a dummy catalog source to assign versions even if source does not exist yet"""
        self._identifier = identifier
        self._catalog = catalog

    def __setitem__(self, index: int, value: Source) -> None:  # noqa D105
        assert index > 0
        self._catalog._sources[self.identifier] = CatalogSource(
            source=value, identifier=self.identifier, catalog=self._catalog, version=index
        )

    def __contains__(self, index: str) -> bool:  # noqa D105
        return False

    @property
    def identifier(self) -> str:
        """Read only property"""
        return self._identifier
