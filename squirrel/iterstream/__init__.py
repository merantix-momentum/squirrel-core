"""
At a low level, squirrel iterstream is defined through a class called :py:class:`Composable`.
All chaining methods aredefined within this class, and can be applied at its high level twin class called
:py:class:`IterableSource`.
"""

from squirrel.iterstream.base import Composable
from squirrel.iterstream.source import FilePathGenerator, IterableSamplerSource, IterableSource

__all__ = ["Composable", "IterableSource", "FilePathGenerator", "IterableSamplerSource"]
