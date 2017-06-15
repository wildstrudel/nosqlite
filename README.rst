nosqlite
=====

nosqlite is a no-sql document-based database wrapper around Python's built-in relational-database module sqlite3

The "database" and "collection" concepts from MongoDB are created around a sqlite3 single-file-based database,
where the "database" may hold multiple different document-based (key-value) "collections", each referred-to by name and
represented by an actual SQL table behind the scenes.

For simplicity's sake, each collection/table holds only two columns - the primary "key" and a pickle-serialized "value".
This allows easy storage, retrieval and iteration of these key-value pairs, but it does not allow complex (or any)
searches of the actual values. The collection also keeps the last-modification timestamp for each key-pair. For items
inserted together, the timestamp will be identical.

This implementation offers a significantly improved performance over other persistent dictionary-like implementations,
such as the 'shelve' module and others, since it relies on the relatively-efficient SQL module sqlite3, which excels 
at handling small-to-medium amounts of data.


Features
--------

- dictionary-like syntax
- multiple key-value CRUD operations
- persistence to a single file on disk
- efficient relative to other no-prequisite solutions
- uses the built-in sqlite3 module


Installation
------------

.. code-block:: bash

	TODO


Usage
-----

.. code-block:: python

# Database and collections creation:

>>> from nosqlite import NoSQLiteDatabase
>>> nsldb = NoSQLiteDatabase(r"c:\temp\example.nsldb")
>>> nsldb.get_collection_names()
[]
>>> col = nsldb.get_or_create_collection("mycollection")
>>> nsldb.get_collection_names()
[u'mycollection']

# Documents set & get:

>>> col.size()
0
>>> col.get("asd")
{}
>>> col.set({"asd": 5})
>>> col.get("asd")
{u'asd': 5}
>>> col.set([("pi", 3.14), ("hello world", u'hello world!'), ("grades", {"john": 3.5, "jim": 4.0, "james": 2})])
>>> col.get("pi")
{u'pi': 3.14}
>>> col.get(["hello world", "grades", "no such key"])
{u'grades': {'james': 2, 'jim': 4.0, 'john': 3.5},
 u'hello world': u'hello world!'}
>>> col.delete(["pi", "no such key"])
>>> col.size()
3

# Dictionary-like syntax:

>>> len(col)
3
>>> col['asd']
5
>>> col['asd2']
---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
<ipython-input-18-03612d4cd3cc> in <module>()
----> 1 col['asd2']
.
.
KeyError: 'asd2'
>>> col["asd2"] = ("testing", 123)
>>> col['asd2']
('testing', 123)
>>> del col["asd2"]
>>> col.keys()
[u'asd', u'hello world', u'grades']
>>> for k,v in col.iteritems(): print k,v
asd 5
hello world hello world!
grades {'john': 3.5, 'jim': 4.0, 'james': 2}
>>> col["newitem"] = complex(1, 2)
>>> for k,v in col.iter_by_date(reverse=True): print k,v
newitem (1+2j)
hello world hello world!
grades {'john': 3.5, 'jim': 4.0, 'james': 2}
asd 5
>>> "newitem" in col
True
>>> "newitem2" in col
False
