"""
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
   
*** Usage examples ***

Database and collections creation:

>>> from nosqlite import NoSQLiteDatabase
>>> nsldb = NoSQLiteDatabase(r"c:\temp\example.nsldb")
>>> nsldb.get_collection_names()
[]
>>> col = nsldb.get_or_create_collection("mycollection")
>>> nsldb.get_collection_names()
[u'mycollection']

Documents set & get:
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

Dictionary-like syntax:
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

Author: wildstrudel
Date: 15/6/2017

"""

import datetime
import sqlite3

try:
    import cPickle as pickle
except ImportError:
    import pickle


class NoSQLiteDatabase(object):
    """
    This class represents a "database" object, which may include multiple "collection" objects, similar to MongoDB
    """

    def __init__(self, filename):
        """
        Constructs a sqlite3 database which will be treated as no-sql "database" object         
        
        :param filename: the name of the database file. If it does not exist, it will be created         
        """
        self.filename = filename
        self.connection = sqlite3.connect(filename)

    def __del__(self):
        self.connection.commit()
        self.connection.close()

    def get_collection_names(self):
        """        
        :return: a list of all existing collection names
        """
        cursor = self.connection.cursor()
        # SQL: get all 'name' fields from the 'sqlite_master' table for 'table' object
        res = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [x[0] for x in res]

    def get_or_create_collection(self, collection_name):
        """
        If the collection exists, returns it. If no - create a new collection and return it

        :param collection_name: the name of the collection
        :return: a NoSQLiteCollection object
        """
        cursor = self.connection.cursor()
        # SQL: get the 'name' field from 'sqlite_master' table for a 'table' object with the given 'name'
        res = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [collection_name, ])
        if len(list(res)) == 0:
            # SQL: create a table with the given name with three columns:
            #  * key (primary) of type TEXT
            #  * value of type BLOB
            #  * ts of type TIMESTAMP
            cursor.execute("CREATE TABLE %s (key TEXT PRIMARY KEY, value BLOB, ts TIMESTAMP)" % collection_name)
        return NoSQLiteCollection(self.connection, collection_name)


class NoSQLiteCollection(object):
    """
        This class represents a "collection" object, similar to MongoDB. Each collection is uniquely identified by name.
        These objects are created by NoSQLiteDatabase, and should not be instantiated independently
        """

    def __init__(self, connection, collection_name):
        """                
        :param connection: this is an established sqlite3 connection created by NoSQLiteDatabase
        :param collection_name: the name of the collection
        """
        self.connection = connection
        self.collection_name = collection_name

    def size(self):
        """        
        :return: the number of documents in the collection 
        """
        cursor = self.connection.cursor()
        # SQL: get the number of different 'key' values in the current collection/table
        res = cursor.execute("SELECT Count(key) FROM %s" % self.collection_name)
        return res.fetchone()[0]

    def __len__(self):
        return self.size()

    def _encode_value(self, v):
        """
        Internal data-encoding function, based on pickle
                
        :param v: picklable Python object 
        :return: a sqlite3.Binary object holding the pickle-endocded data
        """
        return sqlite3.Binary(pickle.dumps(v, pickle.HIGHEST_PROTOCOL))

    def _decode_value(self, d):
        """
        Internal data-decoding function, based on pickle
        :param d: previously-encoded data
        :return: a Python object
        """
        return pickle.loads(str(d))

    def _e_pluribum_unum(self, t):
        """        
        Make sure an object is iterable, to reduce multiple 'if' conditions
        
        :param t: object
        :return: If the given object is a dict, return its '.iteritems()' generator 
                 If the given object is iterable (has the '__iter__' method), simply return it
                 Otherwise return a new tuple around the given object
        """
        if type(t) is dict:
            return t.iteritems()
        elif hasattr(t, '__iter__'):
            return t
        else:
            return (t,)

    def set(self, items):
        """
        Set new values for existing keys and create new key-value pairs for non-existing ones
        
        :param items: an iterable of key-value pairs or dictionary, to be written into the collection
        """
        items = self._e_pluribum_unum(items)
        cursor = self.connection.cursor()
        d = datetime.datetime.now()
        # SQL: update-or-create the row for 'key', with the encoded value as 'value' and the current datetime as
        cursor.executemany("REPLACE INTO %s VALUES (?,?,?)" % self.collection_name, ((key, self._encode_value(value), d) for key,value in items))
        self.connection.commit()

    def __setitem__(self, key, value):
        """
        Syntactic-sugaring for setting a value by-key, either overwriting existing values or creating a now row for
        new keys
        :param key: a key
        :param value: any picklable Python object
        :return: 
        """
        # Convert the key-value parameters to a dictionary before calling 'set'
        self.set({key: value})

    def get(self, keys):
        """
        Get the values of the given keys, if they exist
        
        :param keys: a list of keys to be retrieved
        :return: a dictionary which maps each existing key to its current value (non-existing keys are not returned)
        """
        keys = self._e_pluribum_unum(keys)
        cursor = self.connection.cursor()
        # SQL: get the key and value from the current collection/table for rows with 'key's that are searched for
        res = cursor.execute("SELECT key,value FROM %s WHERE key in (%s)" % (self.collection_name, ','.join(['?',]*len(keys))), keys)
        return dict([(key, self._decode_value(value)) for key,value in res])

    def __getitem__(self, key):
        """
        Syntactic-sugaring for getting the value by-key of a single row, with dictionary-like syntax
        :param key: a key
        :return: the value of the given key's row
        :raises KeyError if the key does not eixst
        """
        res = self.get(key).get(key)
        if res is None:
            raise KeyError(key)
        else:
            return res

    def iterkeys(self):
        """        
        :return: a generator for all keys in the collection, unsorted
        """
        cursor = self.connection.cursor()
        # SQL: get all 'key's from the current collection/table
        res = cursor.execute("SELECT key FROM %s" % self.collection_name)
        for x in res:
            yield x[0]

    def keys(self):
        return list(self.iterkeys())

    def iteritems(self):
        """        
        :return: a generator for all the key-value pairs in the collection, unsorted 
        """
        cursor = self.connection.cursor()
        # SQL: get all keys and values from the current collection/table
        res = cursor.execute("SELECT key,value FROM %s" % self.collection_name)
        for key,value in res:
            yield (key, self._decode_value(value))

    def items(self):
        return list(self.iteritems())

    def __iter__(self):
        return self.iteritems()

    def __contains__(self, key):
        cursor = self.connection.cursor()
        # SQL: attempt to get a given key from the current collection/table, to check its existence
        res = cursor.execute("SELECT key FROM %s WHERE key=?" % self.collection_name, [key,])
        return res.fetchone() is not None

    def delete(self, keys):
        """        
        Delete the given keys' rows, if the exist
        
        :param keys: a list of keys to be deleted
        """
        keys = self._e_pluribum_unum(keys)
        cursor = self.connection.cursor()
        # SQL: delete all the rows with the given keys if they exist from the current collection/table
        cursor.execute("DELETE FROM %s WHERE key in (%s)" % (self.collection_name, ','.join(['?',]*len(keys))), keys)

    def __delitem__(self, key):
        self.delete(key)

    def iter_by_date(self, reverse=False):
        """        
        :param reverse: False returns in ascending order, True with descending order 
        :return: a generator for all the key-value pairs in the collection, sorted by the 'ts' timestamp, in either
                 ascending or descending order
        """
        cursor = self.connection.cursor()
        # SQL: get all keys and values from the current collection/table, and order them by date according to 'ts'
        res = cursor.execute("SELECT key,value FROM %s ORDER BY ts %s" % (self.collection_name, "DESC" if reverse else "ASC"))
        for key,value in res:
            yield (key, self._decode_value(value))
