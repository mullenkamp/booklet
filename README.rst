Booklet
==================================

Introduction
------------
Booklet is a pure python key-value file database. It allows for multiple serializers for both the keys and values. Booklet uses the `MutableMapping <https://docs.python.org/3/library/collections.abc.html#collections-abstract-base-classes>`_ class API which is the same as python's dictionary in addition to some `dbm <https://docs.python.org/3/library/dbm.html>`_ methods (i.e. sync and prune).
It is thread-safe on writes (using thread locks) and multiprocessing-safe (using file locks). Reads are not thread safe.

When an error occurs (e.g. trying to access a key that doesn't exist), booklet will properly close the file and remove the file locks. This will not sync any changes, so the user will lose any changes that were not synced. There will be circumstances that can occur that will not properly close the file, so care still needs to be made.

Installation
------------
Install via pip::

  pip install booklet

Or conda::

  conda install -c mullenkamp booklet


I'll probably put it on conda-forge once I feel like it's up to an appropriate standard...


Serialization
-----------------------------
Both the keys and values stored in Booklet must be bytes when written to disk. This is the default when "open" is called. Booklet allows for various serializers to be used for taking input keys and values and converting them to bytes. There are many in-built serializers. Check the booklet.available_serializers list for what's available. Some serializers require additional packages to be installed (e.g. orjson, zstd, etc). If you want to serialize to json, then it is highly recommended to use orjson or msgpack as they are substantially faster than the standard json python module. If in-built serializers are assigned at initial file creation, then they will be saved on future reading and writing on the same file (i.e. they don't need to be passed after the first time). Setting a serializer to None will not do any serializing, and the input must be bytes.
The user can also pass custom serializers to the key_serializer and value_serializer parameters. These must have "dumps" and "loads" static methods. This allows the user to chain a serializer and a compressor together if desired. Custom serializers must be passed for writing and reading as they are not stored in the booklet file.

.. code:: python

  import booklet

  print(booklet.available_serializers)


Usage
-----
The docstrings have a lot of info about the classes and methods. Files should be opened with the booklet.open function. Read the docstrings of the open function for more details.

Write data using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  import booklet

  with booklet.open('test.blt', 'n', value_serializer='pickle', key_serializer='str', n_buckets=12007) as db:
    db['test_key'] = ['one', 2, 'three', 4]


Read data using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  with booklet.open('test.blt', 'r') as db:
    test_data = db['test_key']

Notice that you don't need to pass serializer parameters when reading (and additional writing) when in-built serializers are used. Booklet stores this info on the initial file creation.

In most cases, the user should use python's context manager "with" when reading and writing data. This will ensure data is properly written and locks are released on the file. If the context manager is not used, then the user must be sure to run the db.sync() (or db.close()) at the end of a series of writes to ensure the data has been fully written to disk. Only after the writes have been synced can additional reads occur. Make sure you close your file or you'll run into file deadlocks!

Write data without using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  import booklet

  db = booklet.open('test.blt', 'n', value_serializer='pickle', key_serializer='str')

  db['test_key'] = ['one', 2, 'three', 4]
  db['2nd_test_key'] = ['five', 6, 'seven', 8]

  db.sync()  # Normally not necessary if the user closes the file after writing
  db.close() # Will also run sync as part of the closing process


Read data without using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  db = booklet.open('test.blt') # 'r' is the default flag

  test_data1 = db['test_key']
  test_data2 = db['2nd_test_key']

  db.close()


Prune deleted items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
When a key/value is "deleted", it's actually just flagged internally as deleted and the item is ignored on the following requests. This is the same for keys that get reassigned. To remove these deleted items from the file completely, the user can run the "prune" method. This should only be performed when the user has done a ton of deletes/overwrites as prune can be computationally intensive. There is no performance improvement to removing these items from the file. It's purely to regain space.

.. code:: python

  with booklet.open('test.blt', 'w') as db:
    del db['test_key']
    db.sync()
    db.prune()


File metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The user can assign overall metadata to the file as a json serializable object (i.e. dict or list). The methods are called set_metadata and get_metadata. The metadata is independent from all of the other key/value pairs assigned in the normal way. It won't be returned with any other methods. If metadata has not already been assigned, the get_metadata method will return None.

.. code:: python

  with booklet.open('test.blt', 'w') as db:
    db.set_metadata({'meta_key1': 'This is stored as metadata'})
    meta = db.get_metadata()


Item timestamps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Timestamps associated with each assigned item have been implemented, but must be turned on at file initialization. By default it's off. The timestamps are stored and returned as an int of the number of microseconds in POSIX UTC time. There are new methods to set and get the timestamps. It's quite new...so I won't supply more info until it's further tested.


Custom serializers
~~~~~~~~~~~~~~~~~~
.. code:: python

  import orjson

  class Orjson:
    def dumps(obj):
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY)
    def loads(obj):
        return orjson.loads(obj)

  with booklet.open('test.blt', 'n', value_serializer=Orjson, key_serializer='str') as db:
    db['test_key'] = ['one', 2, 'three', 4]


The Orjson class is actually already built into the package. You can pass the string 'orjson' to either serializer parameters to use the above serializer. This is just an example of a custom serializer.

Here's another example with compression.

.. code:: python

  import orjson
  import zstandard as zstd

  class OrjsonZstd:
    def dumps(obj):
        return zstd.compress(orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))
    def loads(obj):
        return orjson.loads(zstd.decompress(obj))

  with booklet.open('test.blt', 'n', value_serializer=OrjsonZstd, key_serializer='str') as db:
    db['big_test'] = list(range(1000000))

  with booklet.open('test.blt', 'r', value_serializer=OrjsonZstd) as db:
    big_test_data = db['big_test']

If you use a custom serializer, then you'll always need to pass it to booklet.open for additional reading and writing.


The open flag follows the standard dbm options:

+---------+-------------------------------------------+
| Value   | Meaning                                   |
+=========+===========================================+
| ``'r'`` | Open existing database for reading only   |
|         | (default)                                 |
+---------+-------------------------------------------+
| ``'w'`` | Open existing database for reading and    |
|         | writing                                   |
+---------+-------------------------------------------+
| ``'c'`` | Open database for reading and writing,    |
|         | creating it if it doesn't exist           |
+---------+-------------------------------------------+
| ``'n'`` | Always create a new, empty database, open |
|         | for reading and writing                   |
+---------+-------------------------------------------+

Design
-------
VariableValue (default)
~~~~~~~~~~~~~~~~~~~~~~~~
There are two groups in a booklet file plus some initial bytes for parameters (sub index). The sub index is 200 bytes long, but currently only 37 bytes are used. The two other groups are the bucket index group and the data block group. The bucket index group contains the "hash table". This bucket index contains a fixed number of buckets (n_buckets) and each bucket contains a 6 byte integer of the position of the first data block associated with that bucket. When the user requests a value from a key input, the key is hashed and the modulus of the n_buckets is performed to determine which bucket to read. The 6 bytes is read from that bucket, converted to an integer, then booklet knows where the first data block is located in the file. The data block group contains all of the data blocks each of which contains the key hash, next data block pos, key length, value length, timestamp (if init with timestamps), key, and value (in this order).

The number of bytes per data block object includes:
key hash: 13
next data block pos: 6
key length: 2
value length: 4
timestamp: either 0 (if init without timestamps) or 7
key: variable
value: variable

When the first data block pos is determined through the initial key hashing and bucket reading, the first 19 bytes (key hash and next data block pos) are read. Booklet then checks the next data block pos (ndbp). If the ndbp is 0, then it has been assigned the delete flag and is ignored. The key hash from the data block is compared to the key hash from the input. If they are the same, then this is the data block we want. If they are different, then we look again at the ndbp. If the ndbp is 1, then this is the last data block associated with the key hash and the input key hash doesn't exist. If the ndbp is > 1, then we move to the next data block based on the ndbp and try the cycle again until either we hit a dead end or we find the same key hash.

When we find the identical key hash, Booklet reads 6 bytes (key len and value len) to determine how many bytes are needed to be read to get the key/value (since they are variable). Depending on whether the user wants the key, value, and/or timestamp, Booklet will read 7 bytes (timestamp len) plus the number of bytes for the key and value. 

Deletes assign ndbp to 0 and reassign the prior data block it's original ndbp. This essentially just removes this data block from the key hash data block chain.
A delete also happens when a user "overwrites" the same key.

A "prune" method has been created that allows the user to remove "deleted" items. It has two optional parameters. If timestamps have been initialized in booklet, then the user can pass a timestamp that will remove all items older than that timestamp. The reindexing option allows the user to increase the n_buckets when the number items greatly exceeds the initialized n_buckets. The implementation essentially just clears the original index then iterates through all data blocks and rewrites only the data blocks that haven't been deleted. In the case of the reindexing, it determines the difference between the old index size and the new index size, expands the file by that difference, moves all of the data blocks to the end of the file, and then writes the newer (and longer) index to the file. Then it continues with the normal pruning procedure. 

FixedValue
~~~~~~~~~~~
The main difference from VariableValue is that the value length is globally fixed. The data block in a FixedValue object does not contain the value length as the value will always be the same global value length. The main advantage of this difference is that any overwrites of the same key can be written back to the same location on the file instead of always being appended to the end of the file. If a use-case includes many overwrites and the values are always the same size, then the FixedValue object is ideal.

There are currently no timestamps in the FixedValue. This could be enabled in the future.

Limitations
-----------
The main limitation is that booklet does not have automatic reindexing (increasing the n_buckets). In the current design, reindexing is computationally intensive when the file is large. The user should generally assign an appropriate n_buckets at initialization. This should be approximately the same number as the expected number of keys/values. The default is set at 12007. The "prune" method now has a reindexing option that allows the users to deliberately update/increase the index.

Benchmarks
-----------
From my initial tests, the performance is comparable to other very fast key-value databases (e.g. gdbm, lmdb) and faster than sqlitedict.

