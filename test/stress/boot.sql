


-- check that numeric type works
select __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES integer');
select __spqr__console_execute('CREATE KEY RANGE FROM 300 ROUTE TO sh4 FOR DISTRIBUTION d;');
select __spqr__console_execute('CREATE KEY RANGE FROM 200 ROUTE TO sh3 FOR DISTRIBUTION d;');
select __spqr__console_execute('CREATE KEY RANGE FROM 100 ROUTE TO sh2 FOR DISTRIBUTION d;');
select __spqr__console_execute('CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d;');
select __spqr__console_execute('CREATE DISTRIBUTED RELATION t (i) IN d;');

DROP TABLE IF EXISTS t;

CREATE TABLE t(i INT PRIMARY KEY, j INT);