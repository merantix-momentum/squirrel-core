Catalog
=======
Squirrel provides an easy way of both defining a data source and how to read from it at the same time:

.. code-block:: python

    from squirrel.catalog import Source

    source = Source(
        driver_name="file",
        driver_kwargs={"path": "path/to/my/file"},
        metadata={"owner": "Merantix", "license": "Other"},
    )

That's it, we created our first :py:class:`~squirrel.catalog.source.Source`. Note that:

- Our source can be read using a certain :py:class:`~squirrel.driver.Driver`, i.e. the Driver that corresponds to
  "file".

  .. note::

    Note that each driver defines its name with the ``name`` class variable.
    You can check out :py:class:`~squirrel.driver.FileDriver` to verify that its name is indeed "file".

    You can refer to :py:mod:`squirrel.driver` to see all built-in squirrel drivers.
    To see how you can implement your own driver and register it so that a Catalog can use it, see the `Plugin Tutorial`.

- The driver will be called using some arguments, i.e. ``path="path/to/my/file"``
- We can add metadata to the Source

If we have a lot of Sources, then keeping track of them can be troublesome.
This is where :py:class:`~squirrel.catalog.Catalog` comes in.

Catalog is a dictionary-like data structure that maintains Sources.
Let's create a Catalog and add our Source to it.

.. code-block:: python

    from squirrel.catalog import Catalog

    cat = Catalog()
    cat["my_source"] = source

Now we can see how the Source is stored::

    >>> cat["my_source"]
    {
        "identifier": "my_source",
        "driver_name": "file",
        "driver_kwargs": {
            "path": "path/to/my/file",
        },
        "metadata": {
            "owner": "Merantix",
            "license": "Other"
        },
        "version": 1
        "versions": [
            1
        ]
    }

Note that there are some new fields, more specifically, the source now also has:

- An identifier
- A version

This is because Catalog returns a :py:class:`~squirrel.catalog.CatalogSource`, which is responsible for storing a
single version of a Source.
Inside a Catalog, there can be multiple ``CatalogSource``s for a single data source.
In this case, the ``identifier`` will be the same for all of them but they will each have a unique version number.

We specified the identifier when adding the Source to the Catalog.
However, the version was automatically set.

If we set another source (or the same one in this example) on the same key, 
a new version will automatically be created:

.. code-block:: python

    cat["my_source"] = source  # Automatically add new version of the same source


We could have set the version ourselves as well. So the following would have had the same effect, 
but in this case would explicitly overwrite the existing version:

.. code-block:: python

    cat["my_source"][2] = source  # Explicitly setting or overwriting version 2

Now the catalog entry has become::

    >>> cat["my_source"]

    {
        "identifier": "my_source",
        "driver_name": "file",
        "driver_kwargs": {
            "path": "path/to/my/file",
        },
        "metadata": {
            "owner": "Merantix",
            "license": "Other"
        },
        "version": 2
        "versions": [
            1,
            2
        ]
    }

The catalog returns us the latests version available, which is v2.
It is possible to specify which version to get::

    >>> cat["my_source"][1]  # getting a specific version
    {
        "identifier": "my_source",
        "driver_name": "file",
        "driver_kwargs": {
            "path": "path/to/my/file",
        },
        "metadata": {
            "owner": "Merantix",
            "license": "Other"
        },
        "version": 1
    }

It is possible to get a Driver instance to read from a CatalogSource.

.. code-block:: python

    import tempfile

    # create a dummy file and load it using a FileDriver
    with tempfile.TemporaryDirectory() as tmp_dir:
        fpath = f"{tmp_dir}/myfile.txt"
        with open(fpath, "w") as f:
            for i in range(5):
                f.write(f"line #{i}\n")

        new_source = Source(driver_name="file")
        cat["new_source"] = new_source

        driver = cat["new_source"].get_driver(path=fpath)
        with driver.open() as f:
            f.readlines() # -> ['line #0\n', 'line #1\n', 'line #2\n', 'line #3\n', 'line #4\n']

Catalog Operations
------------------
We will be using the following Catalogs to demonstrate the operations:

.. code-block:: python

    cat1 = Catalog()
    cat2 = Catalog()

    # shared sources
    for i in range(2):
        key, src = f"shared_{i}", Source(driver_name="file")
        cat1[key] = src
        cat2[key] = src

    # distinct sources
    cat1["distinct_for_1"] = Source(driver_name="file")
    cat2["distinct_for_2"] = Source(driver_name="file")

Catalogs can be sliced so that only some sources are left:

.. code-block:: python

    res = cat1.slice(["shared_1"])
    list(res.keys())  # -> ["shared_1"]

Catalogs can be summed together:


.. code-block:: python

    res = cat1.union(cat2)
    list(res.keys())  # -> ['shared_0', 'shared_1', 'distinct_for_1', 'distinct_for_2']

The difference between two catalogs can be taken:

.. code-block:: python

    res = cat1.difference(cat2)
    list(res.keys())  # -> ['distinct_for_1', 'distinct_for_2']

Catalogs can be intersected:

.. code-block:: python

    res = cat1.intersection(cat2)
    list(res.keys())  # -> ['shared_0', 'shared_1']

To see all catalog operations, check out the API reference.

Sharing your Catalog
--------------------

As most things, Catalogs are more fun when shared with others.
To share a Catalog, you must first serialize it.
Luckily, Squirrel provides `Catalog.to_file()` method, which will serialize your catalog for you and write it to a
.yaml file with all information regarding the sources.

.. code-block:: python

    import tempfile

    temp_d = tempfile.TemporaryDirectory()
    fp = f"{temp_d.name}/my_catalog.yaml"
    cat1.to_file(fp)

You can see that all source information is neatly stored in this file::

    >>> with open(fp, "r") as f:
    >>>     for _ in range(10):
    >>>         print(f.readline())
    !YamlCatalog
    version: 0.11.0
    sources:
    - !YamlSource
    identifier: shared_0
    driver_name: MyDriver
    driver_kwargs: {}
    version: 1
    metadata: {}
    - !YamlSource

Reading the file into a catalog is also simple:

.. code-block:: python

    cat_reloaded = Catalog.from_files([fp])
    cat1 == cat_reloaded # -> True

That's it, now you know (almost) everything about Catalogs!

If you are willing to learn more, check out the `Catalog Tutorial` to see some real-world examples or
the `Plugins Tutorial` to see how you can implement and register a new plugin.
You can also refer to the API reference to discover more information such as implementation details.

Don't forget to clean up:

.. code-block:: python

    temp_d.cleanup()
