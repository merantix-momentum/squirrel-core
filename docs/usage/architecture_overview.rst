Architecture overview
=====================

There are four modules in squirrel which are instrumental in understanding the architecture and the overall design:

* :ref:`catalog`: organizing, accessing, and sharing datasets.
* :ref:`driver`: performant and convenient read.
* :ref:`iterstream`: :py:class:`Composable` is the foundational building block in the :ref:`iterstream` module which provides a mechanism to chain iterables, and a fluent api that includes methods such as `map`, `filter`, and `async_map`.
* :ref:`store`: a key/value abstraction for reading data from and writing data to arbitrary storage backends such as filesystem, object store, database, etc.

These modules are designed in a way that `can` be
used together, but this is not enforced in order to maximize flexibility. This may make it difficult to realize the
intended and recommended way of combining squirrel primitives. Although there are many such ways already provided (and many more
that can be implemented for specific use-cases), here we focus on one concrete example that captures the most common
and most widely applicable pattern through a code snippet and its equivalent UML diagram.
Here is a complete data loading pipeline:

.. code-block:: python

    from squirrel.catalog import Catalog

    catalog = Catalog.from_plugins() # Catalog
    train_data = (
        catalog["imagenet"] # CatalogSource
        .get_driver()  # Driver
        .get_iter()  # Composable
        .map(lambda x: transform(x))  # Composable
        .filter(lambda x: filter_func(x))  # Composable
    )  # Composable
    model = YourModelTrainer(train_data).fit()  # e.g. PyTorch DataLoader, XGBoost, etc.


A :py:class:`Catalog` contains zero to many :py:class:`CatalogSource`\s, each of which may be retrieved by an
identifier (or a tuple of an identifier and the version, see :ref:`catalog` for more details). :py:class:`CatalogSource`
contains all necessary information to instantiate an object of type :py:class:`Driver`. :py:class:`Driver` may have
a method :py:meth:`get_iter` which returns an object of type :py:class:`Composable`
(which belongs to :ref:`iterstream` module). `train_data` is an iterable that generates items lazily in a
streaming way for minimal memory footprint.

.. note::
      To access available datasets using `Catalog.from_plugins()`, check out `squirrel-dataset-core repository <https://github.com/merantix-momentum/squirrel-datasets-core>`_.

The following diagram illustrates a (simplified and slightly idealized) view of the relationships between these
classes through one concrete implementation provided by squirrel. Note that here we assume
that the data is in messagepack format (see :ref:`store` for information about different types of store).

.. mermaid::

    classDiagram

        MutableMapping <|-- Catalog
        class Catalog {
            Dict _sources
        }
        Catalog *-- "0..*" CatalogSource
        %% CatalogSource : get_driver()
        class CatalogSource {
            string identifier
            int version
            List~int~ versions

            get_driver() Driver
        }

        class MessagepackDriver {
            string name
            SquirrelStore store

            get(key) Iterable~Dict~
            keys() List~string~
            get_iter() Composable
        }

        %% realiazation
        CatalogSource ..|> MessagepackDriver

        MessagepackDriver ..> Composable
        MessagepackDriver ..> SquirrelStore

        <<abstract>> Composable
        class Composable {
            source Iterable~Any~
        }
        Composable : __iter__() Iterable~Any~
        Composable : map() Composable
        Composable : filter() Composable


        SquirrelStore : set(value, key) None
        SquirrelStore : get(key) Iterable~Any~
        SquirrelStore : keys() Iterable~string~

        SquirrelStore "1" --> MessagepackSerializer
        class MessagepackSerializer {
            serialize(obj)
            deserialize(obj)
            serialize_shard_to_file(obj, fp)
            deserialize_shard_from_file(fp)
        }


The relationships between these components and the methods they provide depends on the particular implementation of
the abstract classes (i.e. :py:class:`Driver`, :py:class:`AbstractStore`, :py:class:`SquirrelSerializer`).
For instance, an implementation of the :py:class:`Drive` may not need to or may choose not to use :py:class:`SquirrelStore`
or :py:class:`Composable` at all.

.. note::

    :py:class:`CatalogSource` is an internal representation of a :py:class:`Source`. For more information on how to
    add a :py:class:`Source` to a catalog, please refer to :ref:`catalog`.
