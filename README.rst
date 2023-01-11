Booklet
==================================

Introduction
------------
Booklet is a pure python key-value file database. It allows for multiple serializers for both the keys and values. The API is designed to use all of the same python dictionary methods python programmers are used to in addition to the typical dbm methods.

Installation
------------
Install via pip::

  pip install booklet

Or conda::

  conda install -c mullenkamp booklet


I'll probably put it on conda-forge once I feel like it's up to an appropriate standard...


Serialization
-----------------------------
Both the keys and values stored in Booklet must be bytes when written to disk. This is the default when "open" is called. Booklet allows for various serializers to be used for taking input keys and values and converting them to bytes. The in-build serializers include pickle, str, json, and orjson (if orjson is installed). If you want to serialize to json, then it is highly recommended to use orjson as it is substantially faster than the standard json python module. If the user has installed the dill python package, it will use this instead of pickle. The dill package will allow the serializers to be more independent from the original source of the serializer classes. Pickle will only reference classes and functions back to the source scripts rather than storing them directly.
The user can also pass custom serializers to the key_serializer and value_serializer parameters. These must have "dumps" and "loads" static methods. This allows the user to chain a serializer and a compressor together if desired.

Usage
-----
The docstrings have a lot of info about the classes and methods. Files should be opened with the booklet.open function. Read the docstrings of the open function for more details.

Write data using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  import booklet

  with booklet.open('test.book', 'n', value_serializer='pickle', key_serializer='str') as db:
    db['test_key'] = ['one', 2, 'three', 4]


Read data using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  with booklet.open('test.book', 'r') as db:
    test_data = db['test_key']

Notice that you don't need to pass serializer parameters when reading. Booklet stores this info on the initial file creation.


Write data without using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  import booklet

  db = booklet.open('test.book', 'n', value_serializer='pickle', key_serializer='str')

  db['test_key'] = ['one', 2, 'three', 4]
  db['2nd_test_key'] = ['five', 6, 'seven', 8]

  db.sync()
  db.close()


Read data without using the context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

  db = booklet.open('test.book', 'r')

  test_data1 = db['test_key']
  test_data2 = db['2nd_test_key']

  db.close()


Recommendations
~~~~~~~~~~~~~~~
In most cases, the user should use python's context manager "with" when reading and writing data. This will ensure data is properly written and (optionally) locks are released on the file. If the context manager is not used, then the user must be sure to run the db.sync() at the end of a series of writes to ensure the data has been fully written to disk. And as with other dbm style APIs, the db.close() must be run to close the file and release locks. MultiThreading is safe for multiple readers and writers, but only multiple readers are safe with MultiProcessing.


Custom serializers
~~~~~~~~~~~~~~~~~~
.. code:: python

  import orjson

  class Orjson:
    def dumps(obj):
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY)
    def loads(obj):
        return orjson.loads(obj)

  with booklet.open('test.book', 'n', value_serializer=Orjson, key_serializer='str') as db:
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

  with booklet.open('test.book', 'n', value_serializer=OrjsonZstd, key_serializer='str') as db:
    db['big_test'] = list(range(1000000))

  with booklet.open('test.book', 'r') as db:
    big_test_data = db['big_test']


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
~~~~~
I need to write a lot more tests for the functionality. I also need to figure out why the prune function does not work...Currently, stale data cannot be removed from a book, but this will be possible in the future.


Benchmarks
-----------
Coming soon...
