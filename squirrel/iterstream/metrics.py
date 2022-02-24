import logging
import typing as t
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


@dataclass
class MetricsConf:
    """
    A config data class controls 3 things: 1) whether the metric IOPS is calculated. 2) whether the metric throughput
    is calculated 3) and which throughput unit is being used.

    Args:
        iops (bool): If true, metrics_iops will be used and calculate
        throughput (bool): If true, metrics_throughput will be used and calculated
        throughput_unit (str): Defaults to `bytes`. Other valid units include 'KB', 'MB', 'GB'.
    """

    iops: bool = True
    throughput: bool = True
    throughput_unit: str = "MB"

    def asdict(self) -> t.Dict:
        """Returns the sample as a dictionary"""
        return asdict(self)


def metrics_iops(count: int, duration: float) -> float:
    """Returns number of items per second based on number of items and duration."""
    return round(count / duration, 2)


def metrics_throughput(size: float, duration: float, unit: str = "bytes") -> float:
    """Returns bytes IO per second based on the total size of items and duration."""
    if unit == "bytes":
        pass
    elif unit == "KB":
        size /= float(1 << 10)
    elif unit == "MB":
        size /= float(1 << 20)
    elif unit == "GB":
        size /= float(1 << 30)
    else:
        logging.warning(
            "Unrecognized units. Only accept values 'KB', 'MB', 'GB' or 'bytes'. Fall back to use bytes instead."
        )
    return round(size / duration, 2)
