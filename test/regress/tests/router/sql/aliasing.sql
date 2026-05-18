
SELECT __spqr__console_execute('CREATE DISTRIBUTION d(int)');
SELECT __spqr__console_execute('CREATE KEY RANGE k0 FROM 0 ROUTE TO sh1');
SELECT __spqr__console_execute('CREATE RELATION sh1.r(i)');
SELECT __spqr__console_execute('CREATE RELATION sh1.r2(i)');
SELECT __spqr__console_execute('CREATE RELATION sh2.r(i)');

CREATE SCHEMA sh1;
CREATE SCHEMA sh2;

CREATE TABLE sh1.r(i int);
CREATE TABLE sh1.r2(i int);
CREATE TABLE sh2.r(i int);

select * from sh1.r join sh1.r2 on true where r.i = 11;

select * from sh1.r join sh2.r on true where r.i = 11;

DROP SCHEMA sh1 CASCADE;
DROP SCHEMA sh2 CASCADE;


SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');