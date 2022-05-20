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


SQUIRREL_PREFIX = ".squirrel_"
SQUIRREL_DIR = ".squirrel"

SQUIRREL_BUCKET = "gs://squirrel-core-public-data"
SQUIRREL_TMP_DATA_BUCKET = "gs://squirrel-core-public-data-tmp"
