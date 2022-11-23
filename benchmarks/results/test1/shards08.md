```
root@denchick-spqr01h:/usr/local/share/sysbench# sysbench  --threads=24 --range-selects=false  --table_size=100000 --auto_inc=false --tables=10 --pgsql-host=localhost --pgsql-port=6432 --db-driver=pgsql --pgsql-user=denchick --pgsql-password=password --pgsql-db=denchick --pgsql-sslmode=disable --verbosity=5 --db-debug=on  --db-ps-mode=disable oltp_read_write.lua run
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
DEBUG: Worker thread (#10) initialized
DEBUG: Worker thread (#4) initialized
DEBUG: Worker thread (#11) initialized
DEBUG: Worker thread (#16) initialized
DEBUG: Worker thread (#7) initialized
DEBUG: Worker thread (#1) initialized
DEBUG: Worker thread (#8) initialized
DEBUG: Worker thread (#21) initialized
DEBUG: Worker thread (#22) initialized
DEBUG: Worker thread (#12) initialized
DEBUG: Worker thread (#13) initialized
DEBUG: Worker thread (#6) initialized
DEBUG: Worker thread (#0) initialized
DEBUG: Worker thread (#18) initialized
DEBUG: Worker thread (#19) initialized
DEBUG: Worker thread (#3) initialized
DEBUG: Worker thread (#17) initialized
DEBUG: Worker thread (#15) initialized
DEBUG: Worker thread (#5) initialized
DEBUG: Worker thread (#2) initialized
DEBUG: Worker thread (#20) initialized
DEBUG: Worker thread (#14) initialized
DEBUG: Worker thread (#23) initialized
DEBUG: Worker thread (#9) initialized
Threads started!

Time limit exceeded, exiting...
(last message repeated 23 times)
Done.

SQL statistics:
    queries performed:
        read:                            191250
        write:                           76500
        other:                           38250
        total:                           306000
    transactions:                        19125  (1911.28 per sec.)
    queries:                             306000 (30580.41 per sec.)
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
    events/s (eps):                      1911.2757
    time elapsed:                        10.0064s
    total number of events:              19125

Latency (ms):
         min:                                    5.41
         avg:                                   12.55
         max:                                   63.94
         95th percentile:                       45.79
         sum:                               240012.03

Threads fairness:
    events (avg/stddev):           796.8750/8.65
    execution time (avg/stddev):   10.0005/0.00
```
