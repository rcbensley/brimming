# Brimming

Your MariaDB database will be brimming with data.

Insert 1 billion rows into 100 tables, using 10,000 row batches, and 100 threads:

    brimming --rows=1000000000 -threads=100 -tables=100 -batch=100000

Alternatively specify the size of data you wish to insert, let's load 1TB:

    brimming --size=1tb --threads=100 --tables=250

## Install

If you have go installed:

    go install github.com/rcbensley/brimming@latest

Otherwise checkout the package tarballs.

## Usage

Other than the usual options like username, password, port etc, all options are related to loading data.
Pay attention to the defaults being populated, which can be reviewed in `brimming --help`.

The default database is called brim. The user will need permissions to create, drop, insert, select on all tables in this database schema, or whatever `--database` is configured to be.

* rows, as mentioned above use either rows or size to specify how much data you wish to load. Internally this is an Int64 value, so the sky's the limit!
* size, internally this is converted into a row count. Each row is roughly 1kb. The option requires a size value followed by the type of unit size. For example, to insert 100mb, use `--size=100mb`.
* tables, how many tables should the data be distributed over? If the batch size is too large, then not all the tables will be loaded.
* batch, how many rows to insert into each table in each insert.
* threads, ramp it up! Can be constrained using the `--connections` flag.
* engine, use a different table engine should you wish.
* connections, limit how many connections are opened to MariaDB.
* skip-drop, do not drop any pre-existing tables.
* skip-count, do not run a `count(*)` on each loaded table at the end of a run.

## Data

What is being loaded? Each row is just over 1KB of random data, 1 billion rows will generate around 1TB of data, exluding indexes.
Brimming does not use prepared statements, just raw auto-committed, multi-value inserts.

The tables are created in numerical order. If you are loading 10 tables, the tables brim1 to brim10 will be created, and dropped if they already exist (unless you are using `--skip-drop`).

The table itself:

    CREATE TABLE IF NOT EXISTS brim1 (
    a bigint(20) NOT NULL AUTO_INCREMENT,
    b int(11) NOT NULL,
    c char(255) NOT NULL,
    d char(255) NOT NULL,
    e char(255) NOT NULL,
    f char(255) NOT NULL,
    PRIMARY KEY (a)) ENGINE=InnoDB;

Quite simple. The column, a, is left to be populated by `AUTO_INCREMENT`. All other fields are populated with randomly generated data in batches.

## Considerations

* The default `max_packet_allowed` is about 16MB, so increase this as you increase your batch size. A warning will be generated if this value is exceeded.
* Do you have the general log or binary logging enabled?
