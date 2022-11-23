```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=100000 --auto_inc=false --tables=10 --pgsql-host=sas-vewvjvext6fqguug.db.yandex.net --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=require --verbosity=5 --db-debug=on  --db-ps-mode=disable oltp_read_write.lua run
sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 24
Initializing random number generator from current time


Initializing worker threads...

DEBUG: Worker thread (#0) started
DEBUG: Worker thread (#1) started
DEBUG: Worker thread (#2) started
DEBUG: Worker thread (#3) started
DEBUG: Worker thread (#4) started
DEBUG: Worker thread (#5) started
DEBUG: Worker thread (#6) started
DEBUG: Worker thread (#7) started
DEBUG: Worker thread (#8) started
DEBUG: Worker thread (#9) started
DEBUG: Worker thread (#10) started
DEBUG: Worker thread (#11) started
DEBUG: Worker thread (#12) started
DEBUG: Worker thread (#13) started
DEBUG: Worker thread (#14) started
DEBUG: Worker thread (#15) started
DEBUG: Worker thread (#16) started
DEBUG: Worker thread (#17) started
DEBUG: Worker thread (#18) started
DEBUG: Worker thread (#19) started
DEBUG: Worker thread (#20) started
DEBUG: Worker thread (#21) started
DEBUG: Worker thread (#22) started
DEBUG: Worker thread (#23) started
DEBUG: Worker thread (#1) initialized
DEBUG: Worker thread (#15) initialized
DEBUG: Worker thread (#19) initialized
DEBUG: Worker thread (#7) initialized
DEBUG: Worker thread (#20) initialized
DEBUG: Worker thread (#14) initialized
DEBUG: Worker thread (#16) initialized
DEBUG: Worker thread (#18) initialized
DEBUG: Worker thread (#4) initialized
DEBUG: Worker thread (#6) initialized
DEBUG: Worker thread (#22) initialized
DEBUG: Worker thread (#2) initialized
DEBUG: Worker thread (#23) initialized
DEBUG: Worker thread (#9) initialized
DEBUG: Worker thread (#8) initialized
DEBUG: Worker thread (#21) initialized
DEBUG: Worker thread (#10) initialized
DEBUG: Worker thread (#17) initialized
DEBUG: Worker thread (#11) initialized
DEBUG: Worker thread (#12) initialized
DEBUG: Worker thread (#13) initialized
DEBUG: Worker thread (#3) initialized
DEBUG: Worker thread (#0) initialized
DEBUG: Worker thread (#5) initialized
Threads started!

Time limit exceeded, exiting...
(last message repeated 23 times)
Done.

SQL statistics:
    queries performed:
        read:                            196160
        write:                           78464
        other:                           39232
        total:                           313856
    transactions:                        19616  (1959.48 per sec.)
    queries:                             313856 (31351.75 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)
DEBUG: 
DEBUG: Query execution statistics:
DEBUG:     min:                                0.0000s
DEBUG:     avg:                                0.0000s
DEBUG:     max:                                0.0000s
DEBUG:   total:                                0.0000s
DEBUG: Results fetching statistics:
DEBUG:     min:                                0.0000s
DEBUG:     avg:                                0.0000s
DEBUG:     max:                                0.0000s
DEBUG:   total:                                0.0000s

Throughput:
    events/s (eps):                      1959.4843
    time elapsed:                        10.0108s
    total number of events:              19616

Latency (ms):
         min:                                    3.42
         avg:                                   12.24
         max:                                   80.33
         95th percentile:                       68.05
         sum:                               240136.18

Threads fairness:
    events (avg/stddev):           817.3333/18.85
    execution time (avg/stddev):   10.0057/0.00
```
