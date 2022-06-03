import json
from dataclasses import asdict, dataclass, field
from typing import Dict

__all__ = ["Source"]


@dataclass(init=True)
class Source:
    driver_name: str
    driver_kwargs: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:  # noqa D105
        return json.dumps(asdict(self), indent=2, default=str)
