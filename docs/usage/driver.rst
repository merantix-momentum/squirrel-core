Driver
======

Driver is the Squirrel component that is used for accessing data.
Combined with the :ref:`usage/iterstream:IterStream` functionalities, drivers provide a powerful and intuitive way of
accessing data:

.. code-block:: python

    from torch.utils.data import DataLoader

    url = "path/to/my/messagepack/dataset"
    driver = MessagepackDriver(url)  # a driver that reads messagepack-serialized data
    train_data = (
        driver.get_iter()  # returns a Composable, i.e. an iterable with 'iterstream' powers
        .filter(lambda dct: not dct["is_bad_sample"])  # skip unwanted samples
        .async_map(augment_image)  # possible to use a thread/process pool, or run on dask
        .batched(size=100)
        .compose(TorchIterable)  # ready for training!
    )

    train_loader = DataLoader(train_data, batch_size=None)
    # ... have fun training your model

You can pass ``storage_options`` to any driver to customize storage backend. Drivers differ in the
way they provide access to data.

.. contents::
    :local:

IterDriver
----------

Most drivers provide a way to iterate over the parts of the underlying data source.
Such drivers inherit from the :py:class:`IterDriver` base class and their :py:meth:`~IterDriver.get_iter` method returns
an iterable of these parts.
Semantically, the "parts" are dataset-dependent and can be anything: a single sentence in a text corpus, or a single
row in a csv file, or a single (image, label) pair in an image classification dataset.

Let's see an IterDriver in action:

.. code-block:: python

    import tempfile

    from squirrel.driver import IterDriver
    from squirrel.iterstream import Composable, IterableSource


    class MyDriver(IterDriver):
        """Driver that loads lines of a text file."""

        name = "my_iter_driver"

        def __init__(self, txt_path: str):
            self.txt_path = txt_path

        def get_iter(self) -> Composable:
            with open(self.txt_path, "r") as f:
                return IterableSource(line.strip() for line in f.readlines())


.. note::

    It is required to define the ``name`` class variable if this driver is intended to be registered with a source in a :ref:`Catalog <usage/catalog:Catalog>`.
    When loading the driver of a source (via the :py:meth:`get_driver` method), the driver name defined in the source
    is checked against the ``name``\s of all available drivers to find the target driver.

    To see how you can register your custom driver so that it can be used with a Catalog, refer to the `Plugin Tutorial`.

.. code-block:: python

    # prepare a text "corpus" and read from it
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("Lorem ipsum dolor sit amet\n")
        f.write("consetetur sadipscing elitr\n")
        f.flush()

        driver = MyDriver(f.name)
        lines = driver.get_iter().collect()
        assert len(lines) == 2
        assert lines[0] == "Lorem ipsum dolor sit amet"
        assert lines[1] == "consetetur sadipscing elitr"

        # get_iter() returns a squirrel.iterstream.Composable, we can use iterstream functionalities directly (actually
        # we already were using collect() above)

        upper_lines = driver.get_iter().map(str.upper).collect()
        assert upper_lines[0] == "LOREM IPSUM DOLOR SIT AMET"
        assert upper_lines[1] == "CONSETETUR SADIPSCING ELITR"

MapDriver
---------

Some data sources inherently has a (key, value) mapping between data parts and some keys identifying these data parts.
Maybe our csv file has an index column that is unique for each row, or our image dataset consists of separate image
files (which are identified by their file names).
In such cases, given a `key`, it is possible to retrieve the corresponding dataset part.
Squirrel provides the :py:class:`MapDriver` base class for this use case:

.. code-block:: python

    import tempfile
    import typing as t

    import pandas as pd

    from squirrel.driver import MapDriver


    class MyDriver(MapDriver):

        name = "my_map_driver"

        def __init__(self, csv_path: str, index_col: str):
            self.csv_path = csv_path
            self.df = pd.read_csv(csv_path, index_col=index_col)

        def get(self, key: str) -> t.Dict:
            return self.df.loc[key].to_dict()

        def keys(self) -> t.Iterator[str]:
            yield from self.df.index


    with tempfile.TemporaryDirectory() as tmp_dir:
        df = pd.get_dummies(list("abca"))
        csv_path = f"{tmp_dir}/dummy.csv"
        df.to_csv(csv_path, index_label="index")

        driver = MyDriver(csv_path, index_col="index")
        sample = driver.get(0)
        assert sample["a"] == 1
        assert sample["b"] == 0
        assert sample["c"] == 0

Even though we only implement the :py:meth:`~MapDriver.get` and :py:meth:`~MapDriver.keys` methods, it is possible to
call :py:meth:`~MapDriver.get_iter` as well. When called, MapDriver takes the keys iterable from :py:meth:`keys` and
will call :py:meth:`get` for each key. See the method reference for more details.

For this simple example, a custom driver works well. In general, it is better to use the :py:class:`CsvDriver` with
.csv files.

StoreDriver
-----------
For common data access scenarios, it is much simpler to delegate low-level data operations to a
:ref:`Store <usage/store:Store>`.
:py:class:`StoreDriver` lets the underlying store to handle :py:meth:`get` and :py:meth:`keys` calls.

For example, :py:class:`MessagepackDriver` can load messagepack-serialized data by using the
:ref:`usage/store:SquirrelStore` behind the scenes.

FileDriver
----------
:py:class:`FileDriver` can be used to access individual files. Let's save and reload a torch model using FileDriver:


.. code-block:: python

    import tempfile

    import torch
    import torch.nn as nn
    import torch.nn.functional as F

    from squirrel.driver.file import FileDriver


    class Model(nn.Module):
        def __init__(self):
            super(Model, self).__init__()
            self.conv1 = nn.Conv2d(1, 20, 5)
            self.conv2 = nn.Conv2d(20, 20, 5)

        def forward(self, x):
            x = F.relu(self.conv1(x))
            return F.relu(self.conv2(x))


    my_model = Model()

    with tempfile.TemporaryDirectory() as temp_dir:
        # trace your model to TorchScript and save using FileDriver
        model_path  = f"{temp_dir}/my_model.pt"

        with FileDriver(model_path).open(mode='wb', create_if_not_exists=True) as f:
            my_scripted_model = torch.jit.script(my_model)
            torch.jit.save(my_scripted_model, f)

        # now, load the model back
        with FileDriver(model_path).open(mode='rb') as f:
            model_reloaded = torch.jit.load(f)

        # test that model outputs are the same
        batch = torch.rand(16, 1, 100,100)
        assert torch.equal(my_model(batch), model_reloaded(batch))

Further reading
---------------
Drivers can be registered as part of a :py:class:`~squirrel.catalog.source.Source` in a
:ref:`Catalog <usage/catalog:Catalog>`.

`squirrel-datasets <https://squirrel-datasets-core.readthedocs.io/en/latest/>`_ provides drivers to load data from
various datasets.
