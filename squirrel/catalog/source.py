import json
from dataclasses import dataclass, field
from typing import Dict, Optional

__all__ = ["Source"]


@dataclass(init=True)
class Source:
    driver_name: str
    driver_kwargs: Optional[Dict[str, str]] = field(default_factory=dict)
    metadata: Optional[Dict[str, str]] = field(default_factory=dict)

    def __repr__(self) -> str:  # noqa D105
        dct = {"driver_name": self.driver_name, "driver_kwargs": self.driver_kwargs, "metadata": self.metadata}
        return json.dumps(dct, indent=2, default=str)
