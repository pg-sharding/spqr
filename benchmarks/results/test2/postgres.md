I ran this test 10 times, the average is 402.42 transactions per second.

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            42960
        write:                           17184
        other:                           8592
        total:                           68736
    transactions:                        4296   (427.24 per sec.)
    queries:                             68736  (6835.88 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      427.2428
    time elapsed:                        10.0552s
    total number of events:              4296

Latency (ms):
         min:                                   10.16
         avg:                                   56.08
         max:                                  185.66
         95th percentile:                       94.10
         sum:                               240906.55

Threads fairness:
    events (avg/stddev):           179.0000/1.22
    execution time (avg/stddev):   10.0378/0.01
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            41980
        write:                           16792
        other:                           8396
        total:                           67168
    transactions:                        4198   (419.16 per sec.)
    queries:                             67168  (6706.57 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      419.1606
    time elapsed:                        10.0153s
    total number of events:              4198

Latency (ms):
         min:                                   11.07
         avg:                                   57.21
         max:                                  189.17
         95th percentile:                       94.10
         sum:                               240164.53

Threads fairness:
    events (avg/stddev):           174.9167/1.68
    execution time (avg/stddev):   10.0069/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            42080
        write:                           16832
        other:                           8416
        total:                           67328
    transactions:                        4208   (419.36 per sec.)
    queries:                             67328  (6709.80 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      419.3623
    time elapsed:                        10.0343s
    total number of events:              4208

Latency (ms):
         min:                                    9.36
         avg:                                   57.20
         max:                                  190.79
         95th percentile:                       95.81
         sum:                               240676.77

Threads fairness:
    events (avg/stddev):           175.3333/2.46
    execution time (avg/stddev):   10.0282/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            41510
        write:                           16604
        other:                           8302
        total:                           66416
    transactions:                        4151   (412.36 per sec.)
    queries:                             66416  (6597.81 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      412.3634
    time elapsed:                        10.0664s
    total number of events:              4151

Latency (ms):
         min:                                   10.64
         avg:                                   58.15
         max:                                  193.24
         95th percentile:                       97.55
         sum:                               241397.97

Threads fairness:
    events (avg/stddev):           172.9583/3.01
    execution time (avg/stddev):   10.0582/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            40140
        write:                           16056
        other:                           8028
        total:                           64224
    transactions:                        4014   (397.70 per sec.)
    queries:                             64224  (6363.21 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      397.7007
    time elapsed:                        10.0930s
    total number of events:              4014

Latency (ms):
         min:                                   10.29
         avg:                                   60.29
         max:                                  198.64
         95th percentile:                      102.97
         sum:                               242008.39

Threads fairness:
    events (avg/stddev):           167.2500/1.85
    execution time (avg/stddev):   10.0837/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            41350
        write:                           16540
        other:                           8270
        total:                           66160
    transactions:                        4135   (411.47 per sec.)
    queries:                             66160  (6583.52 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      411.4699
    time elapsed:                        10.0493s
    total number of events:              4135

Latency (ms):
         min:                                   11.29
         avg:                                   58.29
         max:                                  195.07
         95th percentile:                      101.13
         sum:                               241009.71

Threads fairness:
    events (avg/stddev):           172.2917/1.37
    execution time (avg/stddev):   10.0421/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            40540
        write:                           16216
        other:                           8108
        total:                           64864
    transactions:                        4054   (404.77 per sec.)
    queries:                             64864  (6476.35 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      404.7717
    time elapsed:                        10.0155s
    total number of events:              4054

Latency (ms):
         min:                                   10.64
         avg:                                   59.24
         max:                                  194.23
         95th percentile:                       99.33
         sum:                               240177.26

Threads fairness:
    events (avg/stddev):           168.9167/1.19
    execution time (avg/stddev):   10.0074/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            41370
        write:                           16548
        other:                           8274
        total:                           66192
    transactions:                        4137   (411.65 per sec.)
    queries:                             66192  (6586.36 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      411.6476
    time elapsed:                        10.0499s
    total number of events:              4137

Latency (ms):
         min:                                   10.23
         avg:                                   58.26
         max:                                  196.05
         95th percentile:                      101.13
         sum:                               241009.08

Threads fairness:
    events (avg/stddev):           172.3750/1.32
    execution time (avg/stddev):   10.0420/0.00
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            35610
        write:                           14244
        other:                           7122
        total:                           56976
    transactions:                        3561   (352.97 per sec.)
    queries:                             56976  (5647.48 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      352.9678
    time elapsed:                        10.0887s
    total number of events:              3561

Latency (ms):
         min:                                   11.37
         avg:                                   67.87
         max:                                  389.01
         95th percentile:                      106.75
         sum:                               241674.98

Threads fairness:
    events (avg/stddev):           148.3750/1.44
    execution time (avg/stddev):   10.0698/0.03
```

```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=10000000 --auto_inc=false --tables=100 --pgsql-host=sas-566bcfc19upa2770.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            36920
        write:                           14768
        other:                           7384
        total:                           59072
    transactions:                        3692   (367.52 per sec.)
    queries:                             59072  (5880.37 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

Throughput:
    events/s (eps):                      367.5231
    time elapsed:                        10.0456s
    total number of events:              3692

Latency (ms):
         min:                                   10.36
         avg:                                   65.24
         max:                                  294.43
         95th percentile:                      186.54
         sum:                               240873.82

Threads fairness:
    events (avg/stddev):           153.8333/1.07
    execution time (avg/stddev):   10.0364/0.01
```