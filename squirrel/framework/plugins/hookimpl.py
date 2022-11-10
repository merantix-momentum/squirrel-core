from typing import List, Type

import pluggy

from squirrel.driver.driver import Driver
from squirrel.framework.plugins import PLUGGY_HOOKGROUP

hookimpl = pluggy.HookimplMarker(PLUGGY_HOOKGROUP)


@hookimpl
def squirrel_drivers() -> List[Type[Driver]]:
    """Core drivers of Squirrel."""
    from squirrel.driver import (
        CsvDriver,
        FeatherDriver,
        FileDriver,
        JsonlDriver,
        MessagepackDriver,
        ParquetDriver,
        SourceCombiner,
        StoreDriver,
        ExcelDriver,
        ZarrDriver,
    )

    return [
        CsvDriver,
        FeatherDriver,
        FileDriver,
        JsonlDriver,
        MessagepackDriver,
        ParquetDriver,
        SourceCombiner,
        StoreDriver,
        ExcelDriver,
        ZarrDriver,
    ]
