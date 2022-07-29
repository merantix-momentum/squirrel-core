Store
=====

Architectural Overview
----------------------
In the UML diagram below we show how the SquirrelStore connects to its abstract class and different serializers.

.. mermaid::

    classDiagram

        AbstractStore <|-- SquirrelStore
        SquirrelStore *-- SquirrelSerializer
        SquirrelSerializer <|-- MessagepackSerializer
        SquirrelSerializer <|-- JSONSerializer
        <<abstract>> AbstractStore
        class AbstractStore {

            set(key, value)
            get(key) Iterable~Dict~
            keys() Iterable~Any~
        }

       class SquirrelStore {
           serializer: SquirrelSerializer

       }
        <<abstract>> SquirrelSerializer
       class SquirrelSerializer {

        serialize(obj)
        deserialize(obj)
        serialize_shard_to_file(obj, filepath)
        deserialize_shard_from_file(filepath)
       }

       class MessagepackSerializer {
       }

       class JSONSerializer {
       }

Serialization
--------------
:py:class:`~squirrel.store.SquirrelStore` uses a Serializer to store shards of samples as singular files onto the storage backend (e.g. filesystem, object store, etc.).
Squirrel provides two serializers: :py:class:`~squirrel.serialization.MessagepackSerializer` and :py:class:`~squirrel.serialization.JsonSerializer`.
While JSONL might be preferable for interoperability or being human-readable, Messagepack is
faster to encode and decode and produces smaller files. Messagepack is the recommended format,
unless you have specific constraints or requirements. To demonstrate that Messagepack produces smaller files, we include the code
snippet below. We see that the files are around ~20% smaller compared to JSONL.

.. code-block:: python

  import tempfile
  from pathlib import Path

  import numpy as np

  from squirrel.iterstream import IterableSource
  from squirrel.serialization import JsonSerializer, MessagepackSerializer
  from squirrel.store import SquirrelStore


  # creating random samples
  def get_sample(x):
      return {"img": np.random.random((20, 20, 3)), "label": x}


  N = 100_000

  summary = []

  for ser in [MessagepackSerializer, JsonSerializer]:
      tmpdir = tempfile.TemporaryDirectory()
      store = SquirrelStore(url=tmpdir.name, serializer=ser())
      IterableSource(range(N)).map(get_sample).batched(1000).async_map(store.set).join()  # async writing to store
      size_mb = (
          sum(f.stat().st_size for f in Path(tmpdir.name).glob("**/*") if f.is_file()) / 10e6
      )  # total storage size in mb
      summary.append({"serializer": ser.__name__, "size_mb": size_mb})
      tmpdir.cleanup()
  print(summary)

Output::

    [{'serializer': 'MessagepackSerializer', 'size_mb': 90.6476465}, {'serializer': 'JsonSerializer', 'size_mb': 109.4487942}]

Sharding
--------------
There are several considerations for deciding appropriate shard size:

    #. Parallelizing read and write operation: the higher the number of shards, the bigger the opportunity for parallelizing
       read and write operations. Parallel write may be done with e.g. Spark. For examples, please see:
       `preprocessing with Spark <https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/09.Spark_Preprocessing.ipynb/>`_
       or `SquirrelStore with Spark <https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/07.SquirrelStore_with_Spark.ipynb>`_.

    #. Limit on the memory of the process when writing the shard: The :py:meth:`squirrel.store.Store.set` accepts a shard.
       This means that the whole shard has to be in memory for writing it. While technically shards of any size could have
       been created by incrementally writing to a single shard, we opted for this approach as it makes parallel and distributed write operations easier.

    #. Randomizing during deep learning training: When training deep learning models, for each epoch the order of samples should be randomized.
       Sharding is an important mechanism to achieve semi-random retrieval of samples. To do so, one can simply shuffle the shard keys and then load the content of each.
       That means, the more shards we have, the closer the shuffling process approaches a fully random shuffling.
       There is another mechanism to shuffle samples on the stream by shuffling in the buffer (see :py:meth:`squirrel.iterstream.Composable.shuffle`).
       However, increasing the number of shards is the main idea of increasing the “degree” of randomness.

Custom Stores
--------------
:py:class:`~squirrel.store.AbstractStore` defines an abstraction to provide a key/value API on top of any storage.
All stores should conform to this abstraction.
You may optionally use or implement a :py:class:`~squirrel.serialization.SquirrelSerializer` if you need to serialize your data before persisting.
If you have a specific use-case which is not natively supported
such as reading data via HTTP requests or retrieving from a database, you may need to implement your own Store.
The code snippet below implements a Store connecting to a SQLite database.
Here we can see that the concepts of sharding and serialization are not inherent to Store per se.

.. code-block:: python

  import random
  import sqlite3
  import string
  import tempfile
  import typing as t

  from squirrel.iterstream import IterableSource
  from squirrel.store import AbstractStore


  # generate random letters mapped to a unique key
  def get_key_value() -> t.Tuple[int, str]:
      value = "".join([random.choice(string.ascii_letters) for _ in range(100)])
      return hash(value), value


  class SQLiteStore(AbstractStore):
      def __init__(self, db_name: str):
          self._con = sqlite3.connect(db_name)
          self._cur = self._con.cursor()
          self._cur.execute("DROP TABLE IF EXISTS demo")  # drop existing table and create a simple key-value table
          self._cur.execute("""CREATE TABLE demo (key INTEGER PRIMARY KEY, value TEXT);""")
          self._con.commit()

      def set(self, key: t.Any, value: t.Any) -> None:
          """Insert value given a key."""
          self._cur.execute("INSERT INTO demo VALUES (?,?)", (key, value))
          self._con.commit()

      def get(self, key: t.Any) -> t.Iterable:
          """Retrieve value with the key."""
          return self._cur.execute("SELECT value FROM demo WHERE key=?", key).fetchall()

      def keys(self) -> t.Iterable:
          """Return all the keys stored."""
          return self._cur.execute("SELECT key FROM demo")

      def close(self) -> None:
          """Close the database connection."""
          self._con.close()


  # We create the SQLite db and insert key-value pairs into it
  N = 100_000
  with tempfile.TemporaryDirectory() as temp_dir:
      store = SQLiteStore(f"{temp_dir}/temp.db")
      it = IterableSource(get_key_value() for _ in range(N)).map(lambda x: store.set(*x)).join()
      some_key = next(store.keys())  # retrieve from db using keys
      some_value = store.get(some_key)
      store.close()
