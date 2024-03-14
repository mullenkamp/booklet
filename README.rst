Booklet
==================================

Introduction
------------
Booklet is a pure python key-value file database. It allows for multiple serializers for both the keys and values. Booklet uses the `MutableMapping <https://docs.python.org/3/library/collections.abc.html#collections-abstract-base-classes>`_ class API which is the same as python's dictionary in addition to some `dbm <https://docs.python.org/3/library/dbm.html>`_ methods (i.e. sync and prune).
It is thread-safe (using thread locks on writes) and multiprocessing-safe (using file locks).

Deletes do not remove data from the file directly. Similarly, reassigning a value to an existing key adds a new key/value set to the file. During normal usage, the user will not notice a difference when requesting a key/value set, but the file size will grow. If size becomes an issue because of lots of deletes or reassignments, then the user should run the "prune" method to remove old key/value sets.

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

  with booklet.open('test.blt', 'n', value_serializer='pickle', key_serializer='str') as db:
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


The Orjson class is actually already built into the package. You can pass the string 'orjson' to either serializer parameters to use the above serializer. This is just an example of a serializer.

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


TODO
-----
Starting in version 0.1.8, there is a prune method. It removes "deleted" keys and values from the file, but it currently leaves the old indeces in the hash table. The old indeces should generally not cause a performance issue (and definitely not a file size issue), but it would be nice to have these removed as part of the prune method one day.


Benchmarks
-----------
From my initial tests, the performance is comparable to other very fast key-value databases (e.g. gdbm, lmdb).
Proper benchmarks will be coming soon...
