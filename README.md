With this, your MariaDB database will be brimming with data.

    ./brimming [rows] [threads] [batch_size] [/path/to/maridb.sock]

A table is created per-thread, so 20 threads is 20 tables, with the row count divided between them.
Stick to whole, even numbers.
Binary logging is disabled in the connections session, so no create database, table or insert events are logged.
