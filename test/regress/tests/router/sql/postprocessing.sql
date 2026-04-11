\c spqr-console

CREATE DISTRIBUTION ds1 (varchar HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION r_pp (i HASH MURMUR) FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE r_pp(i TEXT, id INT);

insert into r_pp(i) values('a'),('b'),('c'),('d'),('e'),('f'),('h'),('g'),('k'),('l'),('m'),('o'),('p');

SET __spqr__allow_postprocessing TO false;

SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;

SET __spqr__allow_postprocessing TO true;

SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;

DROP TABLE r_pp;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;