import logging
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import wandb

from squirrel.artifact_manager.base import ArtifactManager, TmpArtifact
from squirrel.catalog import Catalog
from squirrel.catalog.catalog import CatalogSource, Source

logger = logging.getLogger(__name__)


class WandbArtifactManager(ArtifactManager):
    def __init__(self, entity: Optional[str] = None, project: Optional[str] = None, collection: str = "default"):
        """
        Artifact manager using Weights & Biases as backend.

        Aligning this with the FileSystemArtifactManager, the collections correspond to WandB artifact types. However, a
        single WandB artifact can contain multiple files allowing the assignment of a single shared version to groups of
        files.

        Note: For storing objects it is assumed that wandb.init() has been called so that artifacts can be associated
        with a run.
        """
        super().__init__(collection=collection)
        if project is not None:
            self.project = project
        elif wandb.run is not None:
            self.project = wandb.run.project
            logger.info(f"Using project {self.project} from active wandb run.")
        elif wandb.Api().settings["project"] is not None:
            self.project = wandb.Api().settings["project"]
            logger.info(f"Using project {self.project} from wandb settings.")
        else:
            raise ValueError("No project name was provided and no active project could be identified.")
        if entity is not None:
            self.entity = entity
        elif wandb.run is not None and wandb.run.entity is not None:
            self.entity = wandb.run.entity
            logger.info(f"Using entity {self.entity} from active wandb run.")
        elif wandb.Api().settings["entity"] is not None:
            self.entity = wandb.Api().settings["entity"]
            logger.info(f"Using entity {self.entity} from wandb settings.")
        else:
            self.entity = wandb.Api().project(self.project).entity
            logger.info(f"Using entity {self.entity} from wandb project.")

    def list_collection_names(self) -> Iterable:
        """
        List all collections managed by this ArtifactManager.

        Collections correspond to Wandb artifact types.
        """
        return [collection.name for collection in wandb.Api().artifact_types(project=self.project)]

    def exists_in_collection(self, artifact: str, collection: Optional[str] = None) -> bool:
        """
        Check if artifact exists in specified collection.

        Note: This is not supported by the WandB API and therefore requires listing all artifacts in a collection.
        """
        collection = collection or self.active_collection
        if collection not in self.list_collection_names():
            return False
        return artifact in [
            artifact.name
            for artifact in wandb.Api().artifact_type(type_name=collection, project=self.project).collections()
        ]

    def get_artifact_source(
        self,
        artifact: str,
        collection: Optional[str] = None,
        version: Optional[str] = None,
        catalog: Optional[Catalog] = None,
    ) -> CatalogSource:
        """Catalog entry for a specific artifact"""
        if catalog is None:
            catalog = Catalog()
        collection = collection or self.active_collection
        if version is None:
            versions = [
                instance.version
                for instance in wandb.Api().artifact_versions(
                    type_name=collection, name=f"{self.entity}/{self.project}/{artifact}"
                )
            ]
            version = f"v{max([int(v[1:]) for v in versions])}"

        return CatalogSource(
            Source(
                driver_name="wandb",
                driver_kwargs={
                    "url": str(
                        Path(
                            wandb.Api().settings["base_url"],
                            self.entity,
                            self.project,
                            collection,
                            artifact,
                            version,
                        )
                    ),
                },
                metadata={
                    "project": self.project,
                    "collection": collection,
                    "artifact": artifact,
                    "version": version,
                },
            ),
            identifier=str(Path(collection, artifact)),
            catalog=catalog,
            version=int(version[1:]) + 1,  # Squirrel Catalog version is 1-based
        )

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Construct a catalog listing artifacts within a specific collection."""
        collection = collection or self.active_collection
        if collection not in self.list_collection_names():
            raise ValueError(f"Collection {collection} does not exist.")
        artifact_names = [
            artifact.name
            for artifact in wandb.Api().artifact_type(type_name=collection, project=self.project).collections()
        ]
        catalog = Catalog()
        for artifact in artifact_names:
            for instance in wandb.Api().artifact_versions(
                type_name=collection, name=f"{self.entity}/{self.project}/{artifact}"
            ):
                src = self.get_artifact_source(artifact, collection, instance.version, catalog=catalog)
                catalog[str(Path(collection, artifact)), src.version] = src
        return catalog

    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log serialisable object to artifact store."""
        raise NotImplementedError(
            "Logging and retrieving python objects is not yet supported. Please serialize your"
            "objects and log resulting files with 'log_files' or log_folder."
        )
        # Implementation for logging python objects can make use of
        # if not isinstance(obj, wandb.data_types.WBValue):
        #     raise ValueError(
        #         f"Object {obj} is not a wandb serializable object. Please convert to a subclass of"
        #         f" wandb.data_types.WBValue first. See https://docs.wandb.ai/ref/python/data-types/"
        #     )
        # if wandb.run is None:
        #     raise ValueError("No wandb run is active. Please call wandb.init() before logging artifacts.")

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None) -> Any:
        """
        Retrieve specific artifact value.

        This assumes that the artifact was logged as a wandb serialised object. If the artifact was a file upload,
        the file contents should be retrieved using download instead.
        """
        raise NotImplementedError(
            "Logging and retrieving python objects is not yet supported. Please serialize your"
            "objects and retrieve the logged files with 'download_artifact' instead."
        )

    def log_files(
        self,
        artifact_name: str,
        local_path: Path,
        collection: Optional[str] = None,
        artifact_path: Optional[Path] = None,
    ) -> CatalogSource:
        """Upload a single file to artifact store without serialisation."""
        collection = collection or self.active_collection
        artifact = wandb.Artifact(artifact_name, type=collection)
        if artifact_path is not None:
            artifact_path = str(artifact_path)
        if local_path.is_dir():
            artifact.add_dir(str(local_path), name=artifact_path)
        else:
            artifact.add_file(str(local_path), name=artifact_path)
        artifact.save()
        artifact.wait()
        return self.get_artifact_source(artifact_name, collection)

    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None, to: Optional[Path] = None
    ) -> Union[Path, TmpArtifact]:
        """
        Download a specific artifact to a local path.

        WandB serialised objects would be downloaded in a nested folder structure and are therefore discouraged right
        now.
        """
        collection = collection or self.active_collection
        if version is None:
            version = "latest"
        if wandb.run is None:
            art = wandb.Api().artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)
        else:
            art = wandb.run.use_artifact(f"{self.entity}/{self.project}/{artifact}:{version}", type=collection)

        if to is not None:
            art.download(str(to / artifact))
            return to / artifact
        else:
            return TmpArtifact(self, collection, artifact, version)
