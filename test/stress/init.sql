ADD SHARDING RULE r1 TABLE pgbench_accounts COLUMNS aid;

ADD KEY RANGE krid1 FROM 1 TO 50000 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 50001 TO 100000 ROUTE TO sh2;
