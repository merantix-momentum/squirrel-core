import time
import typing as t

import matplotlib.pyplot as plt
import seaborn as sns
from pandas import DataFrame
from squirrel.driver import MessagepackDriver

remote_path = "gs://your-bucket/msgpack-cache-demo-data"
so = {"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}

driver_types = {
    "Caching": lambda: MessagepackDriver(remote_path, storage_options=so),
    "No Caching": lambda: MessagepackDriver(remote_path),
}


def time_epoch(driver_init: t.Callable[[], MessagepackDriver]) -> float:
    """Creates iterator and returns time to fetch all shards."""
    it = driver_init().get_iter().tqdm()
    start = time.time()
    it.join()
    return time.time() - start


def benchmark(driver_types: t.Dict[str, t.Any], num_epochs: int = 10) -> DataFrame:
    """Benchmarks loading speed of the `driver_types` over `num_epochs` epochs."""
    df = DataFrame()
    for k, v in driver_types.items():
        for i in range(num_epochs):
            data = {"Epoch": i + 1, "Time (in s)": time_epoch(driver_init=v), "Driver Type": k}
            df = df.append(data, ignore_index=True)
    return df


df = benchmark(driver_types)

sns.set_theme(style="darkgrid")
sns.set_palette(["#9E36FF", "#11D8C1"])  # company colors

ax = sns.barplot(data=df, x="Epoch", y="Time (in s)", hue="Driver Type")
ax.set(title="Loading 2 shards (~ 100MB each) via MessagepackDriver")
ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))

plt.savefig("advanced/msgpack_caching.svg", bbox_inches="tight")
