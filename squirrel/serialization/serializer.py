from __future__ import annotations

import abc
import typing as t

if t.TYPE_CHECKING:
    from squirrel.constants import SampleType, ShardType


class BaseSerializer(abc.ABC):
    @property
    @abc.abstractmethod
    def file_extension(self) -> str:
        """File extension"""


class SquirrelFileSerializer(BaseSerializer):
    @abc.abstractmethod
    def serialize_shard_to_file(self, obj: ShardType, fp: str, **kwargs) -> None:
        """Serializes a list of samples and writes it to a file."""

    @abc.abstractmethod
    def deserialize_shard_from_file(self, fp: str, **kwargs) -> t.Iterable[SampleType]:
        """Reads a shard from file and returns an iterable over the values of its samples."""


class SquirrelSerializer(SquirrelFileSerializer):
    @abc.abstractmethod
    def serialize(self, obj: t.Any) -> t.Any:
        """Returns the serialized object."""

    @abc.abstractmethod
    def deserialize(self, obj: t.Any) -> t.Any:
        """Returns the deserialized object."""
