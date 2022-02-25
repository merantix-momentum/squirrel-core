Catalog
=======

Squirrel is a pip package which can be installed from our PyPi server using ``pip3 install mxlabs-squirrel``.
It adds the module ``squirrel`` to your environment. The high-level API of squirrel is the Catalog. It allows you to discover and access data sources. 

A Catalog can be instantiated using the Python API, plugins, and Yaml files.

Catalog Python API
--------------------
You can create and manipulate a Catalog using it's Python API. The Catalog acts like a Python Dict[str, Source]. Example:

.. code-block:: python

    from squirrel.catalog import Catalog, Source
    
    # Create catalog
    ca = Catalog()
    ca['test'] = Source("csv", driver_kwargs={'path':'./test.csv'}, metadata={'created': 'yesterday'})

    # Access metadata
    print(ca['test'].metadata)

    # Access underlying dataframe using the specified driver
    df = ca['test'].load.get_df()

    # Versionise source
    ca['test'] = Source("csv", driver_kwargs={'path':'./test2.csv'}, metadata={'created': 'today'})
    df = ca['test'] # latest version
    df = ca['test'][-1] # latest version
    df = ca['test'][1] # first version
    df = ca['test'][2] # second version
    print(ca['test'].versions) # list versions

    # A Catalog supports set operations e.g.
    ca2 = Catalog()
    ca2['v'] = Source("csv", driver_kwargs={'path':'./test.csv'})
    ca3 = ca.union(ca2)
    ca4 = ca.intersection(ca2)
    ca5 = ca.difference(ca2)

    # You can filter the catalog by a predicate
    ca6 = ca.filter(lambda x: x.driver_name == 'csv')

    # Persist catalog to yaml
    ca.to_file('./test.yaml')


Source plugins
--------------------

Sources can be added to the catalog via a plugin mechanism. The intended use case is sharing of sources within a project or via distribution of a Python package (e.g. mxlabs-datasets). You can inject sources via pluggy or on the fly like in this example:

.. code-block:: python

    from squirrel.catalog import Catalog, Source
    from squirrel.framework.plugins.plugin_manager import register_source

    s = Source("csv", driver_kwargs={'path':'./test.csv'})
    register_source('cs', s)

    ca = Catalog.from_plugins()
    df = ca['cs'].load.get_df()

Catalog Yaml Files
--------------------

You can also create a Catalog based on a yaml. The intended use case is easy sharing of automatically created datasets (e.g. from a ETL pipeline). Squirrel can scan local and remote locations for Catalog files (e.g. Catalog.from_dirs()). Example for a local Catalog file and data source.

Create a catalog.yaml file:

.. code-block:: yaml

    !YamlCatalog
    version: 0.4.0
    sources:
    - !YamlSource
      identifier: cs
      driver_name: csv
      driver_kwargs:
        path: ./test.csv

This Catalog defines a data source 'cs' from a local CSV file. To create the corresponding dataframe:

.. code-block:: python

    from squirrel.catalog import Catalog

    cat = Catalog.from_files(['./catalog.yaml'])
    source = cat["ca"]
    df = source.load.get_df()


Driver plugins
--------------------

You can inject drivers for your custom data types via pluggy or on the fly like in this example:

.. code-block:: python

    from squirrel.catalog import Catalog, Source
    from squirrel.driver import Driver
    from squirrel.framework.plugins.plugin_manager import register_driver

    class MyDriver(Driver):
        name='mydriver'

        def __init__(self, name, **kwargs):
            
            super().__init__(*kwargs)
            self.name = name

        def say_hi(self, **kwargs):
            return f"Hello {self.name}!"
    register_driver(MyDriver)

    ca = Catalog()
    ca['test'] = Source("mydriver", driver_kwargs={'name':'Labs'})
    print(ca['test'].load.say_hi())