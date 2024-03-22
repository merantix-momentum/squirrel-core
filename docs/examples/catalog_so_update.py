from typing import Type

from squirrel.catalog import Catalog, Source
from squirrel.driver import CsvDriver, Driver

# user 1 creates a catalog, saves it, and shares it with user 2
cat = Catalog()
cat["source"] = Source(
    "csv",
    driver_kwargs={
        "url": "gs://some-bucket/test.csv",
        "storage_options": {"requester_pays": True},
    },
)
cat.to_file("catalog.yaml")

# user 2 loads catalog from file and inserts their storage_options
cat = Catalog.from_files(["catalog.yaml"])
driver: Type[Driver] = cat["source"].get_driver(
    storage_options={
        "protocol": "simplecache",
        "target_protocol": "gs",
        "cache_storage": "path/to/cache",
    }
)

# storage_options from user 1 and user 2 are merged
assert isinstance(driver, CsvDriver)
assert driver.storage_options == {
    "requester_pays": True,
    "protocol": "simplecache",
    "target_protocol": "gs",
    "cache_storage": "path/to/cache",
}
