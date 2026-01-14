
# Slick-DuckDB

Slick extension for DuckDB.

TODO: version of DuckDB, what's supported syntax wise
TODO: what's not supported, e.g.:
  - blob limitation: handled as byte array, so caution advised for byte array sizing; watch memory consumption and performance
  - check constraints are not supported


## How to use it

TODO: how to install

TODO: an example how to set up database connection and query
TODO: Database creation factories
TODO: in-memory tests, but also file-based db tests

TODO: Implement DatabaseMetaData.getTypeInfo endpoint in DuckDB JDBC Driver
TODO: "Check constraints" support



CheckInsertOrUpdateBuilder & UpdateInsertBuilder
These two builders are only used when insertOrUpdate is emulated on the client side.
InsertOrUpdate emulation is basically doing UPDATE WHERE CONDITION first and then INSERT ... WHERE NOT EXISTS (SELECT 1 from table where condition)
One can do this client side or transactionally on the server side.
Server side emulation is faster but may still fail due to concurrent updates.
Postgres driver for example does this server side.
The scala doc says that when doing client side, the two Builders would need to be used.
For DuckDB we don't need to use client side emulation, because we can either implement server side emulation or a native upsert.

UpsertBuilder
This one is complicated.
MySQL driver implements it natively by using a similar INSERT ON DUPLICATE as opposed to DuckDBs INSERT ON CONFLICT syntax.
The Postgres driver implements the server side emulation. The pg-slick external driver has its own native upsert implementation.
An additional complication here is auto-incremental columns, especially auto-incremental primary keys

