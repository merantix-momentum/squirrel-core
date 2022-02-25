Glossary
==============

Squirrel has it's own terminology which is outlined here.

.. glossary::

    Catalog
      A dictionary like collection of :term:`Sources<Source>`.

    Source
      Defines a data source including metadata, how it was created, and how it can be used.

    DataLoader
      An interface describing how to load the underlying data.

    DataLoader Specification
      A dictionary like description to initalize a :term:`DataLoader`.

    Driver
      An abstract class defining an access mode that is implemented by a :term:`DataLoader`

    Record
      A dictionary like dataclass object which describing a sample in a dataset.

    Record Fetcher
      A mechanism to retrieve or write a :term:`Record`.

    Shard
      A collection of :term:`Records<Record>` which are usually stored as a single file to reduce costs.

    IterStream
      An API for conveniently chaining iterators and iterables.

    Store
      A key-value store abstraction used for persisting :term:`Records<Record>`.