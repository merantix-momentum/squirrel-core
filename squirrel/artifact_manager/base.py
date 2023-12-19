from abc import abstractmethod, ABC
import logging
from pathlib import Path
import re
import tempfile
from types import TracebackType
from typing import Optional, Any, Iterable, Type

from squirrel.catalog import Catalog, Source

logger = logging.getLogger(__name__)


class DirectoryLogger:
    """
    Class to be used as a context for logging a directory as an artifact.
    When entering the scope it creates a local dir with a valid afid and returns the filepath to it.
    You can then write files to that dir and after exiting the scope the dir gets logged through the artifact manager.
    """

    def __init__(self, artifact_manager: "ArtifactManager", artifact: str, collection: Optional[str]) -> None:
        """
        Initializes the DirectoryLogger.

        Args:
            artifact_manager: An artifact manager instance to use for logging the final artifact.
            artifact: The name of the artifact to log.
            collection: The name of the collection to log to, defaults to the active collection of the artifact manager.
        """
        self.artifact_manager = artifact_manager
        self.artifact = artifact
        self.collection = collection
        self.tempdir = tempfile.TemporaryDirectory()

    def __enter__(self) -> str:
        """
        Called when entering the context. Creates folder under /tmp with the artifacts id if it doesn't exist yet.

        Returns: Absolute path to artifact folder as str
        """
        Path(self.tempdir.name, self.artifact).mkdir(exist_ok=False)
        return str(Path(self.tempdir.name, self.artifact))

    def __exit__(
        self,
        exctype: Optional[Type[BaseException]] = None,
        excinst: Optional[BaseException] = None,
        exctb: Optional[TracebackType] = None,
    ) -> None:
        """Called when exiting the context. Logs the artifact folder if it's not empty."""

        path = Path(self.tempdir.name) / Path(self.artifact)
        files = [p for p in path.glob("*")]
        if len(files) > 0:
            self.artifact_manager.log_files(self.artifact, path, self.collection)
        else:
            logger.info(f"Did not log artifact folder at {path} as it seems to be empty.")

        if self.tempdir is not None:
            self.tempdir.cleanup()


class ArtifactManager(ABC):
    def __init__(self):
        """Artifact manager interface for various backends."""
        self._active_collection = "default"

    @property
    def active_collection(self) -> str:
        """
        Collections act as folders of artifacts.

        It is ultimately up to the user how to structure their artifact store and the collections therein. All
        operations accessing artifacts allow explicit specification of the collection to use.
        Therefore, users could use collections to separate different artifact types or different experiments / runs.
        The manager maintains an 'active' collection which it logs to by default. This can be set at the run start
        when the manager is initialized and then remain unchanged for the duration of the run.

        To avoid incompatibility between different backends, collections cannot be nested (e.g. as subfolders on a
        filesystem) as in particular the WandB backend has no real notion of nested folder structures.
        """
        return self._active_collection

    @active_collection.setter
    def active_collection(self, value: str) -> None:
        """
        Sets the active collections that is being logged to by default.

        The provided value is verified to ensure that no nested collections are used.
        """
        if not re.match(r"^[a-zA-Z0-9\-_:]+$", value):
            raise ValueError(
                "Invalid collection name - must not be empty and can only contain alphanumerics, dashes, underscores "
                "and colons."
            )
        self._active_collection = value

    @abstractmethod
    def list_collection_names(self) -> Iterable:
        """Return list of all collections in the artifact store"""
        raise NotImplementedError

    @abstractmethod
    def exists_in_collection(self, artifact: str, collection: Optional[str] = None) -> bool:
        """Check if artifact exists in specified collection."""
        raise NotImplementedError

    @abstractmethod
    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Catalog of all artifacts within a specific collection."""

    @abstractmethod
    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None) -> Any:
        """Retrieve specific artifact value."""
        raise NotImplementedError

    @abstractmethod
    def log_files(
        self,
        artifact_name: str,
        local_path: Path,
        collection: Optional[str] = None,
        artifact_path: Optional[Path] = None,
    ) -> Source:
        """
        Upload a file or folder into (current) collection, increment version automatically

        Args:
            artifact_name: Name of artifact to log
            local_path: Local path to the file or folder to log
            collection: Name of collection to log to, defaults to the active collection
            artifact_path: path under which to log the files within the artifact, defaults to "./"
        """
        raise NotImplementedError

    @abstractmethod
    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """
        Log an arbitrary python object

        The serialisation method used is backend dependent. When using a simple FileStore backend any SquirrelSerializer
        can be chosen. For WandB objects serialisation is handled by WandB itself.
        """
        raise NotImplementedError

    @abstractmethod
    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None, to: Path = "./"
    ) -> Source:
        """Retrieve file (from current collection) to specific location. Retrieve latest version unless specified."""
        raise NotImplementedError

    def exists(self, artifact: str) -> bool:
        """Check if artifact exists in specified collection."""
        return any([self.exists_in_collection(artifact, collection) for collection in self.list_collection_names()])

    def store_to_catalog(self) -> Catalog:
        """Provide Catalog of all artifacts stored in backend."""
        catalog = Catalog()
        for collection in self.list_collection_names():
            catalog = catalog.union(self.collection_to_catalog(collection))
        return catalog

    def log_folder(self, artifact: str, collection: Optional[str] = None) -> DirectoryLogger:
        """Create a context manager for logging a directory of files as a single artifact."""
        return DirectoryLogger(self, artifact, collection)

    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        """Download all artifacts in collection to local directory."""
        if collection is None:
            collection = self.active_collection
        catalog = self.collection_to_catalog(collection)
        for _, artifact in catalog:
            artifact_name = artifact.metadata["artifact"]
            self.download_artifact(artifact_name, collection, to=to / artifact_name)
        return catalog
