Shelflet
==================================

Introduction
------------
Shelflet reimplements `shelve <https://docs.python.org/3/library/shelve.html>`_ with several improvements for multiple serializers and compressors. The API is designed to use all of the same python dictionary methods python programmers are used to.


Installation
------------
Install via pip::

  pip install shelflet

Or conda::

  conda install -c mullenkamp shelflet


I'll probably put it on conda-forge once I feel like it's up to an appropriate standard...


Serialization and compression
-----------------------------
The keys in shelflet also have their own serializer option (key_serializer). Setting this option to 'str' for example will allow strings as keys, but any of the serializers can be used for keys. No compressors are allowed for keys (at the moment).
The values stored in shelflet must be bytes objects. But to be as flexible as possible (in the spirit of the shelve module), shelflet provides multiple serialization options to convert values of many object types to bytes. The most obvious and flexible option is pickle. Other options for serialization in shelflet include the str, json, and orjson. If your values can be serialized to json, then it is highly recommended to use the orjson option as it's really fast and you don't have to worry about pickle objects backwards compatibility issues.

shelflet also provides several compression options for taking the serialized objects and compressing them. These include gzip, zstd, and lz4. zstd and lz4 are very fast compressors and are the recommended compressors. Nevertheless, compression should be tested to verify that there is a significant reduction in file size as compared to no compression. If not, then you're wasting performance.

The defaults in shelflet have been set to not do serialization and compression as to require the user to make a conscious decision on these options.

Custom user defined serializer and compressor classes can also be passed to the respective parameter names in the open function. Look at the main.py "Serializers and compressors" section for examples.

Usage
-----
The docstrings have a lot of info about the classes and methods. Files should be opened with the shelflet.open function. Read the docstrings of the open function for more details.

Write data
~~~~~~~~~~
.. code:: python

  import shelflet

  with shelflet.open('test.shelf', 'n', serializer='pickle', key_serializer='str') as db:
    db['test_key'] = ['one', 2, 'three', 4]


Read data
~~~~~~~~~
.. code:: python

  with shelflet.open('test.shelf', 'r') as db:
    test_data = db['test_key']

Notice that you don't need to pass serializer or compressor parameters when reading. shelflet stores this info on the initial file creation.

Recommendations
~~~~~~~~~~~~~~~
In most cases, the user should use python's context manager "with" when reading and writing data. This will ensure data is properly written and (optionally) locks are released on the file. If the context manager is not used (and the default sync=False), then the user must be sure to run the db.sync() at the end of a series of writes to ensure the data has been fully written to disk. And as with other dbm style APIs, the db.close() must be run to close the file and release locks. MultiThreading is safe for multiple readers and writers, but only multiple readers are safe with MultiProcessing. If MultiThreading operations are expected, then opening a file with lock=True is necessary.


Benchmarks
-----------
Coming soon...

Possible future backends
------------------------
libmdbx
~~~~~~~
Supposedly an improvement over lmdb and subsequently meant to function similarly. The lmdb backend for python has been found to have high CPU usage and data corruption.
An important improvement is that databases can be reorganized in-place to reclaim space. lmdb requires you to copy an existing database to a new file to do this.

Main repo? But it is currently archived...:
https://github.com/erthink/libmdbx

Rough python bindings:
https://gitlab.com/Thermi/py3-libmdbx
