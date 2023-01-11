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
Both the keys and values stored in Booklet must be bytes when written to disk. This is the default when "open" is called. Booklet allows for various serializers to be used for taking input keys and values and converting them to bytes. The in-build serializers include pickle, str, json, and orjson (if orjson is installed). If you want to serialize to json, then it is highly recommended to use orjson as it is substantially faster than the standard json python module.
The user can also pass custom serializers to the key_serializer and value_serializer parameters. These must have "dumps" and "loads" static methods. This allows the user to chain a serializer and a compressor together if desired.

Usage
-----
The docstrings have a lot of info about the classes and methods. Files should be opened with the booklet.open function. Read the docstrings of the open function for more details.

Write data
~~~~~~~~~~
.. code:: python

  import booklet

  with booklet.open('test.book', 'n', value_serializer='pickle', key_serializer='str') as db:
    db['test_key'] = ['one', 2, 'three', 4]


Read data
~~~~~~~~~
.. code:: python

  with booklet.open('test.book', 'r') as db:
    test_data = db['test_key']

Notice that you don't need to pass serializer parameters when reading. Booklet stores this info on the initial file creation.

Recommendations
~~~~~~~~~~~~~~~
In most cases, the user should use python's context manager "with" when reading and writing data. This will ensure data is properly written and (optionally) locks are released on the file. If the context manager is not used, then the user must be sure to run the db.sync() at the end of a series of writes to ensure the data has been fully written to disk. And as with other dbm style APIs, the db.close() must be run to close the file and release locks. MultiThreading is safe for multiple readers and writers, but only multiple readers are safe with MultiProcessing.


Benchmarks
-----------
Coming soon...
