import logging
import random
import threading
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from time import time
from typing import List, Optional, Set

import pandas as pd
import random_name
from fsspec.implementations.local import LocalFileSystem

from squirrel.constants import APPEND_LOG_DIR, FILESYSTEM
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.zarr.key import key_end

logger = logging.getLogger(__name__)


@dataclass
class ALogRow:
    """Schema for a row in Append Log."""

    key: str  # AppendLog does not restrict the definition of a key here, any str could work
    operation: int  # only supports +1 (upsert), and -1 (remove) but extensible per user request
    timestamp: float = field(default_factory=time)  # timestamp, if not given, then use the current time.
    sizeof: Optional[int] = None  # size of the item corresponding to the key above.

    def __post_init__(self):
        """Validate `operation`."""
        if self.operation not in (1, -1):
            raise ValueError(
                f"'operation' in append log only accepts the values 1 and -1, yet {self.operation} is given."
            )


# A dataclass can be converted into a row in pandas by pd.DataFrame([ALogRow(operation="...", key="...")])
# pydantic basemodel is not supported by pandas yet, see https://github.com/pandas-dev/pandas/pull/27999
# for their updated support for dataclass, but for BaseModel, this is not supported


def dataclass_to_schema(dataclass: dataclass) -> dict:
    """Returns a dict with keys being the fields of a dataclass, and values being their type hints."""
    return {field.name: field.type for field in fields(dataclass)}


ALogSchema = dataclass_to_schema(ALogRow)
ALogColumns = list(ALogSchema.keys())  # get column names for a row in append log.
ALogTypes = list(ALogSchema.values())  # get type hints for a row in append log.


class AppendLog:
    def __init__(
        self,
        url: str,
        fs: FILESYSTEM = None,
        writing_row_threshold: int = 10 ** 4,  # about 600 KB in df.
        reading_row_threshold: int = 2 * 1024 ** 2,  # to support imagenet size data.
    ) -> None:
        """
        Records time events of all upsert and remove key operations for squirrel caching (consolidation). Already added
        inside SquirrelGroup, such that whenever a modification inside a zarr group occurs, this event (in particular,
        the associated key) will be logged under '.squirrel/append_log`.

        Internally, it records two operations 'upsert' and 'remove' (coded as 1, -1 respectively), and their
        corresponding file path keys in a pandas DataFrame. After reaching a preset threshold `row_threshold`,
        it will flush the in-memory table onto disk (in csv files), and after you have done all operations, call
        `AppendLog.close()` to flush all residue data in-memory onto disk.

        Additionally, AppendLog provides a `read()` method to read all logs under `SQUIRREL_DIR` to a pandas dataframe
        until a preset threshold is reached (`reading_row_threshold`). Such that the user dont have to implement extra
        things to read the log files.

        Args:
            url (str): The url to the dataset root group, under which the squirrel caching folder
                (`SQUIRREL_DIR`) will be set.
            fs (FILESYSTEM): One of the fsspec filesystems.
            writing_row_threshold (int): Maximal number of rows written in a single df, before flush them onto disk.
            reading_row_threshold (int): Maximal number of rows in df when trying to read all csv into one df in memory.
        """

        self.row_threshold = writing_row_threshold  # N.B. writing_row_threshold only controls writing ops
        self.read_row_threshold = reading_row_threshold  # N.B. reading_row_threshold only controls reading ops
        url = url.rstrip("/")
        self.root_ds_url = url
        self.alog_url = f"{url}/{APPEND_LOG_DIR}"
        if fs is None:
            self.fs = get_fs_from_url(self.alog_url)

        self.lock = threading.RLock()

        # session name, used to avoid different user / session clashing with each other
        random.seed()
        self.session_name = random_name.generate_name()

        self.csv_prefix = "alog"

        # init an empty set of metadata
        self.current_csv = None
        self.df_log = None

        # init df and csv
        self.counter = -1  # a counter for thread to count number of page.
        # point to the head of csv in self.csvs, set to be 0 index, therefore the sentinel counter = -1
        self._get_new_page()

        logger.debug("Successfully initialized an append log for squirrel.")

    # main methods

    def append(self, row: ALogRow) -> None:
        """Append a new row to AppendLog."""
        with self.lock:  # In particular, lock the if statement to avoid data loss led by thread racing.
            # (When a racing occurs, the thread may determine it wrong whether a new page has already created or not,
            # sometimes writes a new page onto an old csv.)
            # the quickest way to append a row inplace, instead of df.append
            self.df_log.loc[len(self.df_log.index)] = asdict(row)
            # flush to disk if df len reaches threshold
            if len(self.df_log.index) >= self.row_threshold:
                self.flush()

    def flush(self, end_of_session: bool = False) -> None:
        """Flush df in-memory to disk csv file."""
        with self.lock:
            # fix FileNotExistError when working with fsspec.local
            if isinstance(self.fs, LocalFileSystem) and not self.fs.exists(self.current_csv.rsplit("/", 1)[0]):
                Path(self.current_csv).parent.mkdir(parents=True)

            # flush to csv
            self.df_log.to_csv(self.current_csv, mode="a", header=None, index=False)
            logger.debug(f"Finished flushing df_alog into disk file '{key_end(self.current_csv)}'.")
            if not end_of_session:
                self._get_new_page()

    def read(self) -> pd.DataFrame:
        """Return all csv append log records in one df, until reading_row_threshold reaches."""
        # TODO add pandas to partial read the entire csv base.
        csvs = self._load_csvs_on_disk()
        logger.debug(f"Found the following append logs on disk: {[key_end(k) for k in csvs]}.")

        if not csvs:
            raise FileNotFoundError(f"No append log found at '{self.alog_url}'.")
        with self.fs.open(csvs[0], "rb") as f:
            df = pd.read_csv(f, names=ALogColumns)
        for csv_f in csvs[1:]:
            # append df from each csv until reaches reading_row_threshold
            with self.fs.open(csv_f, "rb") as f:
                df_new = pd.read_csv(f, names=ALogColumns)
            df = pd.concat([df_new, df], ignore_index=True)
            if len(df.index) >= self.read_row_threshold:
                logger.warning(
                    f"Reading df from disk csvs exceeding threshold {self.read_row_threshold}"
                    "return the current agg df without reading more csvs."
                )
                return df  # df size will slightly exceed reading_row_threshold, but for simplicity
        return df

    def reconstruct_all_keys(self) -> Set[str]:
        """Return all keys reconstructed from append logs."""
        df = self.read()
        keys = self._get_keys_from_df(df)
        return keys

    def reconstruct_key_cluster(self, prefix: str) -> Set[str]:
        """Return all keys with a certain prefix, reconstructed from append logs. Note that including the root dataset
        url inside the prefix is optional. Append Log will automatically parse to the same result in both situations. In
        other words, passing the absolute path to a zarr subgroup or only the relative path (with respect to the root
        group url) as the prefix are both fine for this method.

        Args:
            prefix (str): The prefix of a particular key cluster.
        """
        df = self.read()
        keys = self._get_keys_from_df(df, prefix)
        return keys

    def compact(self) -> None:
        """Compact all underlying csv files for append log."""
        # TODO external sort all csv files.
        return NotImplementedError

    # entry and exit points

    def close(self) -> None:
        """Close appendlog, flush residue in-memory df to disk."""
        logger.info("Exiting AppengLog.")
        self.flush(end_of_session=True)

    # helper methods

    def _get_new_page(self) -> None:
        """Set the current pointer to a new page (that is, a new csv and a new df)."""
        self.counter += 1
        self.current_csv = self._full_csv_path()
        self.df_log = pd.DataFrame(columns=ALogColumns)
        logger.debug(f"Created a new page for squirrel append log, currently at csv '{key_end(self.current_csv)}'.")
        # set a new df with the above-mentioned ALogRow schema

    def _full_csv_path(self) -> str:
        """Return a constructed full path key for a csv log file."""
        thread_id = threading.get_ident()
        thread_name = f"thread_{thread_id}"
        return f"{self.alog_url}/{self.csv_prefix}_{self.session_name}_{thread_name}_{self.counter}.csv"

    def _load_csvs_on_disk(self) -> List[str]:
        """Load csv files from disk."""
        return [k for k in self.fs.ls(self.alog_url + "/") if k.endswith(".csv")]

    @staticmethod
    def _get_keys_from_df(df: pd.DataFrame, prefix: str = None) -> Set[str]:
        df = df[df["key"].str.startswith(prefix)] if prefix is not None else df
        df = (
            df.sort_values(by="timestamp", ignore_index=True)
            .groupby("key")
            .tail(1)  # only need the lastest change
            .query("operation == 1")  # select operation == upsert, disregard keys get removed.
        )
        return set(df["key"])
