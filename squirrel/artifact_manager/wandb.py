from pathlib import Path
from typing import Optional, Any, Union, List, Iterable

from squirrel.artifact_manager.base import ArtifactManager
from squirrel.catalog import Catalog, Source


class WandbArtifactManager(ArtifactManager):
    """Dummy placeholder for now

    TODO: Implement this based on the wandb Artifact API
    """
    def list_collection_names(self) -> Iterable:
        pass

    def store_to_catalog(self) -> Catalog:
        pass

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        pass

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None) -> Any:
        pass

    def get_artifact_source(self, artifact: str, collection: Optional[str] = None,
                            version: Optional[int] = None) -> Source:
        pass

    def get_artifact_location(self, artifact_name: str, collection: Optional[str] = None,
                              version: Optional[int] = None) -> str:
        pass

    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        pass

    def log_collection(self, files: Union[Path, List[Path]], collection_name: Optional[str] = None) -> Catalog:
        pass

    def log_object(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        pass

    def download_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None,
                          to: Path = "./") -> Source:
        pass

    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        pass

    def __init__(self):
        super().__init__()
