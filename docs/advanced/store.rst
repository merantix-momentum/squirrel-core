Store
=====
TODO: Focus on SquirrelStore
As you have previously learned :py:class:`Store` is an abstraction for efficient storage and retrieval of data from
a filesystem. It is mostly used as a lower-level component of :py:class:`StoreDriver`.
Looking under the hood, the efficiency of :py:class:`Store` are mainly comes from two components:

* Sharding: Storing and retrieving chunks of data
* `MessagePack Serialization <https://github.com/msgpack/msgpack-python>`_: Like a faster and more efficient JSON

In this section, we will learn about these two components.
Lastly, we will also take a look at how :py:class:`Store` deals with any kind of filesystem including cloud storage.

Serialization
--------------
Each Store instance defines a serialization protocol. In general, it is encouraged to use the MessagePack standard
as it offers faster loading and a more compact representation over JSONL.
Squirrel comes with both :py:class:`MessagepackSerializer` and :py:class:`JsonSerializer` defined.

If you want to define your own Serializer, you have to inherit from the py:class:`SquirrelSerializer` base class and implement
all the its methods. Here is an example on how to create a protobuf serializer:


Sharding
--------------
Sharding is simply the practice of storing and loading data into chunks, where each chunk is identified
by a unique key. This has the advantage of reducing read and write operations to disk. In :py:class:`SquirrelStore` sharding
is simply implemented as serializing and storing a list of values as a file with an unique key as its filename.

FileSystems
--------------
