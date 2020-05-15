With this, your MariaDB database will be brimming with data.

    ./brimming 10000 20 100

A table is created per-thread, so 10,000 rows and 20 threads is 20 tables with 500 rows in each table. A batch size of 100 means 100 rows will be inserted at a time. Currently, each row is unique.

Each row is just over 1KB excluding the index (a bigint primary key, and a secondary index on a random int).
The goal of brimming is to load up to 1TB of data at per-run. Runtime will vary greatly depeninding your hardware and resource allocation in 'my.cnf'

A row count of 1 billion will generate around 1TB of row data, exluding indexes!

## Install

    go get github.com/rcbensley/brimming

## Options
Stick to whole, even numbers for everything.
All DML and DDL statements have sql_log_bin disabled, so none of what brimming creates is in the binlog.

    ./brimming [rows] [threads] [batch_size] [/path/to/maridb.sock]

**rows**, a max of 1 billion / 1,000,000,000 / 1000000000, min of 1 row.

**threads**, a max of 100, min of 1.

**batch size**, there is now max here, but it can't exceed the row row. Min of 1.

**socket path**, this does not need to be specified if you socket is already at the default path of "/var/lib/mysql/mysql.sock".
