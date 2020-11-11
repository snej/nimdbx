# nimdbx

An extremely fast persistent key-value store for Nim, based on [libMDBX](https://github.com/erthink/libmdbx).

## The Data Model

Nimdbx manages **databases** stored as regular files.
Each database can contain multiple **collections**.
Each collection can contain any number of **keys** and their associated **values**.
A collection lets you get and set values for keys.
A **cursor** lets you iterate a collection's keys and values in order.

### Keys and Values

Values are just arbitrary blobs, up to 4GB long.

Keys are arbitrary blobs, up to about 1KB long. By default, keys are sorted using lexical order (i.e. using `memcmp`); alternatively, keys can be interpreted as 32-bit or 64-bit integers and sorted accordingly. Each collection can have its own key format.

A collection may support **duplicate keys**: multiple records with the same key and distinct values. This is useful when using collections to implement indexes.

## Status

This is pretty new code (as of November 2020). It has tests, but hasn't been exercised much yet.
libMDBX itself is pretty solid, though.

Not all the functionality of libMDBX is exposed. I'll be adding more as time goes on.
