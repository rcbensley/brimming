Your MariaDB database will be brimming with data.

Insert 1 billion rows into 100 tables, 10k batches a time, using 100 threads:

    brimming -rows=1000000000 -threads=100 -tables=100 -batch=100000

Each row is just over 1KB.
1 billion rows will generate around 1TB of data, exluding indexes!

## Install

    go get github.com/rcbensley/brimming

