from typing import List, Tuple, Type

import pluggy

from squirrel.catalog.catalog import CatalogKey
from squirrel.catalog.source import Source
from squirrel.driver.driver import Driver
from squirrel.framework.plugins import PLUGGY_HOOKGROUP

hookspec = pluggy.HookspecMarker(PLUGGY_HOOKGROUP)


@hookspec
def squirrel_sources() -> List[Tuple[CatalogKey, Source]]:
    """Hook for plugins to add sources to Catalog.from_plugins.

    Returns:
        List of (CatalogKey, source) tuples.
    """
    pass


@hookspec
def squirrel_drivers() -> List[Type[Driver]]:
    """Hook for plugins to add drivers that can be used in Catalogs."""
    pass
