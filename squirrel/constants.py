from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

from fsspec.spec import AbstractFileSystem

PREPROCESSING_SERVICE_ACCOUNT = "preprocessor@merantix-squirrel.iam.gserviceaccount.com"

URL = Union[str, Path]
FILESYSTEM = Union[MutableMapping, AbstractFileSystem]
MetricsType = Dict[str, Union[float, int]]
SampleType = Dict[str, Any]
ShardType = List[SampleType]
SeedType = Optional[Union[int, float, str, bytes, bytearray]]

RECORD = Dict[str, Any]

SQUIRREL_PREFIX = ".squirrel_"
SQUIRREL_DIR = ".squirrel"

APPEND_LOG_DIR = f"{SQUIRREL_DIR}/append_log"

LOGGER_PROJECT_NAME = "squirrel"
MLFLOW_TRACKING_URI = "http://localhost:5000"
