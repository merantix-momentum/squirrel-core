import json
from dataclasses import dataclass, field
from typing import Dict, Optional

__all__ = ["Source"]


@dataclass(init=True)
class Source:
    """Defines a data source by describing its metadata and how it can be read.

    The driver specified within the Source is responsible for reading from the data source and should contain all the
    necessary logic for reading, Source itself does not contain this logic.

    Sources can be set in a :py:class:`~squirrel.catalog.Catalog` along with a key and a version that uniquely identify
    them.
    """

    driver_name: str
    driver_kwargs: Optional[Dict[str, str]] = field(default_factory=dict)
    metadata: Optional[Dict[str, str]] = field(default_factory=dict)

    def __repr__(self) -> str:  # noqa D105
        dct = {"driver_name": self.driver_name, "driver_kwargs": self.driver_kwargs, "metadata": self.metadata}
        return json.dumps(dct, indent=2, default=str)
