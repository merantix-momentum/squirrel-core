from __future__ import annotations

from typing import TYPE_CHECKING

import pluggy

from squirrel.catalog.catalog import CatalogKey
from squirrel.framework.plugins import hookimpl, hookspec

if TYPE_CHECKING:
    from squirrel.catalog.source import Source
    from squirrel.driver import Driver

squirrel_plugin_manager = pluggy.PluginManager(hookspec.PLUGGY_HOOKGROUP)
squirrel_plugin_manager.add_hookspecs(hookspec)

# register core plugin
squirrel_plugin_manager.register(hookimpl, "squirrel")

# register any other installed plugins
squirrel_plugin_manager.load_setuptools_entrypoints(hookspec.PLUGGY_HOOKGROUP)


def register_source(identifier: str, source: Source, version: int = 1) -> None:
    """Add Source to Squirrel so that it is loaded in Catalog.from_plugins."""

    class MyPlugin:
        @hookimpl.hookimpl
        def squirrel_sources(self) -> list[tuple[CatalogKey, Source]]:
            return [(CatalogKey(identifier, version), source)]

    squirrel_plugin_manager.register(MyPlugin())


def register_driver(driver: type[Driver]) -> None:
    """Add Driver to Squirrel so that it can be used in catalogs."""

    class MyPlugin:
        @hookimpl.hookimpl
        def squirrel_drivers(self) -> list[type[Driver]]:
            return [driver]

    squirrel_plugin_manager.register(MyPlugin())


def list_driver_names() -> list[str]:
    """List names of registered drivers"""
    plugins = squirrel_plugin_manager.hook.squirrel_drivers()
    ret = []
    for plugin in plugins:
        ret += [driver.name for driver in plugin]
    return ret
