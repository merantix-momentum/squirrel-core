from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import ruamel.yaml

from squirrel import __version__ as sq_ver
from squirrel import catalog

__all__ = ["prep_yaml", "catalog2yamlcatalog", "yamlcatalog2catalog"]


def prep_yaml() -> ruamel.yaml.YAML:
    """Returns a ruamel.yaml.YAML instance with preregistered classes of squirrel catalogs."""
    yaml = ruamel.yaml.YAML()
    yaml.register_class(YamlSource)
    yaml.register_class(YamlCatalog)
    yaml.register_class(catalog.CatalogKey)
    return yaml


@dataclass
class YamlSource:
    identifier: str
    driver_name: str
    driver_kwargs: Dict[str, str] = field(default_factory=dict)
    version: int = 1
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class YamlCatalog:
    version: str = sq_ver
    sources: List[YamlSource] = field(default_factory=list)


def catalog2yamlcatalog(catobj: catalog.Catalog) -> YamlCatalog:
    """Convert a Catalog into a serializable catalog DTO"""
    sources = [
        YamlSource(
            identifier=iden,
            driver_name=source.driver_name,
            driver_kwargs=source.driver_kwargs,
            version=source.version,
            metadata=source.metadata,
        )
        for iden in catobj.keys()
        for _ver, source in catobj.get_versions(iden).items()
    ]
    return YamlCatalog(sources=sources)


def yamlcatalog2catalog(yamlcat: YamlCatalog) -> catalog.Catalog:
    """Convert a serializable catalog DTO into a Catalog"""
    yamlcat = YamlCatalog(**yamlcat.__dict__)  # recreate to set default values

    # check version
    ver_split = [int(x) for x in yamlcat.version.split(".")]
    assert ver_split[0] > 0 or (ver_split[0] == 0 and ver_split[1] >= 4)

    cat = catalog.Catalog()
    for s in yamlcat.sources:
        s_cp = YamlSource(**s.__dict__)  # recreate to set default values
        # flat list to version graph
        cat[s_cp.identifier, s_cp.version] = catalog.Source(
            driver_name=s_cp.driver_name,
            driver_kwargs=dict(**s_cp.driver_kwargs),
            metadata=dict(**s_cp.metadata),
        )
    return cat
