from dataclasses import dataclass, field
from typing import Dict, List

import ruamel.yaml

from squirrel import __version__ as sq_ver
from squirrel.catalog import Catalog, CatalogKey, Source

__all__ = ["prep_yaml", "catalog2yamlcatalog", "yamlcatalog2catalog"]


def prep_yaml() -> ruamel.yaml.YAML:
    """Returns a ruamel.yaml.YAML instance with preregistered classes of squirrel catalogs."""
    yaml = ruamel.yaml.YAML()
    yaml.register_class(YamlSource)
    yaml.register_class(YamlCatalog)
    yaml.register_class(CatalogKey)
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


def catalog2yamlcatalog(catobj: Catalog) -> YamlCatalog:
    """Convert a Catalog into a serializable catalog DTO"""
    sources = []
    # flatten version graph
    for k, s in catobj.sources.items():
        for sv in s.versions.values():
            sources.append(
                YamlSource(
                    identifier=k,
                    driver_name=sv.driver_name,
                    driver_kwargs=sv.driver_kwargs,
                    version=sv.version,
                    metadata=sv.metadata,
                )
            )
    return YamlCatalog(sources=sources)


def yamlcatalog2catalog(yamlcat: YamlCatalog) -> Catalog:
    """Convert a serializable catalog DTO into a Catalog"""
    yamlcat = YamlCatalog(**yamlcat.__dict__)  # recreate to set default values

    # check version
    ver_split = [int(x) for x in yamlcat.version.split(".")]
    assert ver_split[0] > 0 or (ver_split[0] == 0 and ver_split[1] >= 4)

    cat = Catalog()
    for s in yamlcat.sources:
        s_cp = YamlSource(**s.__dict__)  # recreate to set default values
        # flat list to version graph
        cat[s_cp.identifier][s_cp.version] = Source(
            driver_name=s_cp.driver_name,
            driver_kwargs=dict(**s_cp.driver_kwargs),
            metadata=dict(**s_cp.metadata),
        )
    return cat
