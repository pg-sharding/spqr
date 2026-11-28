\c spqr-console

CREATE DISTRIBUTION ds1 (varchar HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION r_pp (i HASH MURMUR) FOR DISTRIBUTION ds1;
CREATE DISTRIBUTED RELATION r_p (i HASH MURMUR) FOR DISTRIBUTION ds1;

\c regress

SET search_path TO 'public';
CREATE TABLE r_pp(i TEXT, id INT);

insert into r_pp(i) values('a'),('b'),('c'),('d'),('e'),('f'),('h'),('g'),('k'),('l'),('m'),('o'),('p');

SET __spqr__allow_postprocessing TO false;

SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;

SET __spqr__allow_postprocessing TO true;

SELECT 1 AS tie FROM r_pp ORDER BY 1 FETCH FIRST 1 ROW WITH TIES;
SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;

SELECT count(1) FROM r_pp;
SELECT count(*) FROM r_pp;
SELECT count(i) FROM r_pp;
SELECT count(i), count(1), count(*) FROM r_pp;

SELECT SUM(length(i)) FROM r_pp;

SELECT SUM(DISTINCT length(i)) FROM r_pp;
-- XXX: support later?
SELECT count(DISTINCT id) AS n FROM r_pp;

-- should be rejected in router
SELECT count(id), i FROM r_pp GROUP BY i;

CREATE TABLE r_p(i TEXT, id INT);

insert into r_p(i) values('a'),('b'),('c'),('d'),('e'),('h'),('g'),('k'),('l'),('m'),('o'),('p');
insert into r_p(i, id) values('f', 1);

SET __spqr__allow_postprocessing TO false;
SELECT EXISTS(SELECT 1 FROM r_p WHERE id = 1);

SET __spqr__allow_postprocessing TO true;
SELECT EXISTS(SELECT 1 FROM r_p WHERE id = 1);

DROP TABLE r_pp;
DROP TABLE r_p;


/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
