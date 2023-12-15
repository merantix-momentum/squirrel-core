import itertools
from pathlib import Path
from typing import Optional, Any, Iterable

import logging
import wandb

from squirrel.artifact_manager.base import ArtifactManager
from squirrel.catalog import Catalog, Source

logger = logging.getLogger(__name__)


class WandbArtifactManager(ArtifactManager):
    def __init__(self, project: Optional[str] = None):
        """
        Artifact manager using Weights & Biases as backend.

        Aligning this with the FileSystemArtifactManager, the collections correspond to WandB artifact types. However, a
        single WandB artifact can contain multiple files allowing the assignment of a single shared version to groups of
        files.

        Note: This assumes that wandb.init() has been called before initializing this class.
        """
        super().__init__()
        if project is not None:
            self.project = project
        elif wandb.run is not None:
            self.project = wandb.run.project
        else:
            raise ValueError("No project name was provided and no wandb run is active.")
        if wandb.Api().settings["entity"] is not None:
            self.entity = wandb.Api().settings["entity"]
        else:
            self.entity = wandb.Api().project(self.project).entity

    def list_collection_names(self) -> Iterable:
        """
        List all collections managed by this ArtifactManager.

        Collections correspond to Wandb artifact types.
        """
        return itertools.chain(
            *[
                [artifact.name for artifact in collection.collections()]
                for collection in wandb.Api().artifact_types(project=self.project)
            ]
        )

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None) -> Any:
        """
        Retrieve specific artifact value.

        This assumes that the artifact was logged as a wandb serialised object. If the artifact was a file upload,
        the file contents should be retrieved using download instead.
        """
        if collection is None:
            collection = self._active_collection
        if version is None:
            version = "latest"
        if wandb.run is None:
            artifact = wandb.Api().artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)
        else:
            artifact = wandb.run.use_artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)

        value = artifact.get("data")
        if value is not None:
            return value
        else:
            raise ValueError(f"Artifact {artifact} was a file upload, please use download instead.")

    def get_artifact_source(
        self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None
    ) -> Source:
        """Catalog entry for a specific artifact"""
        if collection is None:
            collection = self.active_collection
        if version is None:
            version = "latest"

        return Source(
            driver_name="wandb",
            driver_kwargs={
                "url": Path(
                    wandb.Api().settings["base_url"],
                    self.entity,
                    self.project,
                    collection,
                    artifact,
                    version,
                ),
            },
            metadata={
                "project": self.project,
                "collection": collection,
                "artifact": artifact,
                "version": version,
                "location": Path(
                    wandb.Api().settings["base_url"], self.entity, self.project, collection, artifact, version
                ),
            },
        )

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Construct a catalog listing artifacts within a specific collection."""
        if collection is None:
            collection = self._active_collection
        artifact_names = [
            artifact.name
            for artifact in wandb.Api().artifact_type(type_name=collection, project=self.project).collections()
        ]
        catalog = Catalog()
        for artifact in artifact_names:
            for instance in wandb.Api().artifact_versions(type_name=collection, name=f"{self.project}/{artifact}"):
                catalog[str(Path(collection, artifact))] = self.get_artifact_source(
                    artifact, collection, instance.version
                )
        return catalog

    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        """Upload a single file to artifact store without serialisation."""
        if collection is None:
            collection = self._active_collection
        artifact = wandb.Artifact(name, type=collection)
        artifact.add_file(str(local_path), "data")
        artifact.save()
        artifact.wait()
        return self.get_artifact_source(name, collection)

    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log serialisable object to artifact store."""
        if not isinstance(obj, wandb.data_types.WBValue):
            raise ValueError(
                f"Object {obj} is not a wandb serializable object. Please convert to a subclass of"
                f" wandb.data_types.WBValue first. See https://docs.wandb.ai/ref/python/data-types/"
            )
        if wandb.run is None:
            raise ValueError("No wandb run is active. Please call wandb.init() before logging artifacts.")
        if collection is None:
            collection = self._active_collection
        artifact = wandb.Artifact(name, type=collection)
        artifact.add(obj, "data")
        artifact.save()
        artifact.wait()
        return self.get_artifact_source(name, collection, artifact.version)

    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None, to: Path = "./"
    ) -> Source:
        """
        Download a specific artifact to a local path.

        WandB serialised objects would be downloaded in a nested folder structure and are therefore discouraged right
        now.
        """
        if collection is None:
            collection = self._active_collection
        if version is None:
            version = "latest"
        if wandb.run is None:
            art = wandb.Api().artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)
        else:
            art = wandb.run.use_artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)

        if art.get("data") is not None:
            logger.warning(
                "Artifact was a serialised object, the directory structure was created by WandB and may contain "
                "nested folders."
            )
            art.download(str(to))

        art.download(str(to))
        return Source(
            driver_name="file",
            driver_kwargs={"url": str(to)},
            metadata={"collection": collection, "artifact": artifact, "version": version, "location": str(to)},
        )
