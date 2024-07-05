create distribution ds1 column types integer;

alter distribution ds1 attach relation pgbench_branches distribution key bid;
alter distribution ds1 attach relation pgbench_tellers distribution key tid;
alter distribution ds1 attach relation abalance distribution key aid;
alter distribution ds1 attach relation pgbench_accounts distribution key aid;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
