I ran this test 10 times, the average is 373.76 transactions per second.

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            36400
        write:                           14560
        other:                           7280
        total:                           58240
    transactions:                        3640   (362.47 per sec.)
    queries:                             58240  (5799.57 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      362.4731
    time elapsed:                        10.0421s
    total number of events:              3640

Latency (ms):
         min:                                   14.32
         avg:                                   66.15
         max:                                  289.20
         95th percentile:                      144.97
         sum:                               240803.76

Threads fairness:
    events (avg/stddev):           151.6667/1.65
    execution time (avg/stddev):   10.0335/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            36630
        write:                           14652
        other:                           7326
        total:                           58608
    transactions:                        3663   (363.89 per sec.)
    queries:                             58608  (5822.31 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      363.8941
    time elapsed:                        10.0661s
    total number of events:              3663

Latency (ms):
         min:                                   12.95
         avg:                                   65.88
         max:                                  302.62
         95th percentile:                      186.54
         sum:                               241307.39

Threads fairness:
    events (avg/stddev):           152.6250/1.07
    execution time (avg/stddev):   10.0545/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            36190
        write:                           14476
        other:                           7238
        total:                           57904
    transactions:                        3619   (360.23 per sec.)
    queries:                             57904  (5763.64 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      360.2276
    time elapsed:                        10.0464s
    total number of events:              3619

Latency (ms):
         min:                                   13.48
         avg:                                   66.56
         max:                                  295.58
         95th percentile:                      186.54
         sum:                               240893.49

Threads fairness:
    events (avg/stddev):           150.7917/1.00
    execution time (avg/stddev):   10.0372/0.01
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            37650
        write:                           15060
        other:                           7530
        total:                           60240
    transactions:                        3765   (374.73 per sec.)
    queries:                             60240  (5995.71 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      374.7322
    time elapsed:                        10.0472s
    total number of events:              3765

Latency (ms):
         min:                                   12.70
         avg:                                   63.99
         max:                                  283.19
         95th percentile:                      153.02
         sum:                               240923.19

Threads fairness:
    events (avg/stddev):           156.8750/0.97
    execution time (avg/stddev):   10.0385/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            37540
        write:                           15016
        other:                           7508
        total:                           60064
    transactions:                        3754   (368.80 per sec.)
    queries:                             60064  (5900.75 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      368.7968
    time elapsed:                        10.1790s
    total number of events:              3754

Latency (ms):
         min:                                   13.09
         avg:                                   64.79
         max:                                  285.84
         95th percentile:                      179.94
         sum:                               243235.11

Threads fairness:
    events (avg/stddev):           156.4167/1.38
    execution time (avg/stddev):   10.1348/0.06
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            38540
        write:                           15416
        other:                           7708
        total:                           61664
    transactions:                        3854   (383.99 per sec.)
    queries:                             61664  (6143.78 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      383.9865
    time elapsed:                        10.0368s
    total number of events:              3854

Latency (ms):
         min:                                   13.75
         avg:                                   62.45
         max:                                  276.54
         95th percentile:                      112.67
         sum:                               240682.49

Threads fairness:
    events (avg/stddev):           160.5833/1.50
    execution time (avg/stddev):   10.0284/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            37990
        write:                           15196
        other:                           7598
        total:                           60784
    transactions:                        3799   (374.45 per sec.)
    queries:                             60784  (5991.28 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      374.4548
    time elapsed:                        10.1454s
    total number of events:              3799

Latency (ms):
         min:                                   12.90
         avg:                                   63.56
         max:                                  284.38
         95th percentile:                      183.21
         sum:                               241462.84

Threads fairness:
    events (avg/stddev):           158.2917/1.24
    execution time (avg/stddev):   10.0610/0.03
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            38500
        write:                           15400
        other:                           7700
        total:                           61600
    transactions:                        3850   (382.85 per sec.)
    queries:                             61600  (6125.67 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      382.8547
    time elapsed:                        10.0560s
    total number of events:              3850

Latency (ms):
         min:                                   12.49
         avg:                                   62.63
         max:                                  280.98
         95th percentile:                      147.61
         sum:                               241133.62

Threads fairness:
    events (avg/stddev):           160.4167/0.81
    execution time (avg/stddev):   10.0472/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            38760
        write:                           15504
        other:                           7752
        total:                           62016
    transactions:                        3876   (384.91 per sec.)
    queries:                             62016  (6158.51 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      384.9070
    time elapsed:                        10.0700s
    total number of events:              3876

Latency (ms):
         min:                                   12.86
         avg:                                   62.19
         max:                                  269.31
         95th percentile:                      114.72
         sum:                               241067.49

Threads fairness:
    events (avg/stddev):           161.5000/1.55
    execution time (avg/stddev):   10.0445/0.01
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            38740
        write:                           15496
        other:                           7748
        total:                           61984
    transactions:                        3874   (381.27 per sec.)
    queries:                             61984  (6100.35 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      381.2720
    time elapsed:                        10.1607s
    total number of events:              3874

Latency (ms):
         min:                                   12.29
         avg:                                   62.84
         max:                                  280.37
         95th percentile:                      179.94
         sum:                               243426.36

Threads fairness:
    events (avg/stddev):           161.4167/1.35
    execution time (avg/stddev):   10.1428/0.03
```