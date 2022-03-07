from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from fsspec.spec import AbstractFileSystem

URL = Union[str, Path]
FILESYSTEM = Union[MutableMapping, AbstractFileSystem]
MetricsType = Dict[str, Union[float, int]]
SampleType = Dict[str, Any]
ShardType = List[SampleType]
SeedType = Optional[Union[int, float, str, bytes, bytearray]]

RECORD = Dict[str, Any]

SQUIRREL_PREFIX = ".squirrel_"
SQUIRREL_DIR = ".squirrel"

LOGGER_PROJECT_NAME = "squirrel"
MLFLOW_TRACKING_URI = "http://localhost:5000"
