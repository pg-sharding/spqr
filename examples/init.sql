create distribution ds1 column types integer;

alter distribution ds1 attach relation pgbench_branches distribution key bid;
alter distribution ds1 attach relation pgbench_tellers distribution key tid;
alter distribution ds1 attach relation abalance distribution key aid;
alter distribution ds1 attach relation pgbench_accounts distribution key aid;
alter distribution ds1 attach relation pgbench_history distribution key tid;

CREATE KEY RANGE FROM 3000 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 2000 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 1000 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
