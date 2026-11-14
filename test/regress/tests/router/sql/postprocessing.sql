\c spqr-console

CREATE DISTRIBUTION ds1 (varchar HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION r_pp (i HASH MURMUR) FOR DISTRIBUTION ds1;
CREATE DISTRIBUTED RELATION r_ties (i HASH MURMUR) FOR DISTRIBUTION ds1;

\c regress

SET search_path TO 'public';
CREATE TABLE r_pp(i TEXT, id INT);
CREATE TABLE r_ties(i TEXT, k TEXT);

insert into r_pp(i) values('a'),('b'),('c'),('d'),('e'),('f'),('h'),('g'),('k'),('l'),('m'),('o'),('p');

SET __spqr__allow_postprocessing TO false;

SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;
SELECT i FROM r_pp ORDER BY i FETCH FIRST 1 ROW WITH TIES;

SET __spqr__allow_postprocessing TO true;

SELECT 1 ORDER BY 1 FETCH FIRST 0 ROWS WITH TIES;
SELECT 1 AS tie FROM r_pp ORDER BY 1 FETCH FIRST 1 ROW WITH TIES;
INSERT INTO r_ties VALUES ('a', 'a'), ('b', 'a'), ('c', 'b'), ('d', 'b'), ('e', 'c');
SELECT k FROM r_ties ORDER BY k FETCH FIRST 1 ROW WITH TIES;
SELECT k FROM r_ties ORDER BY k OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT k FROM r_ties ORDER BY k OFFSET 1 /* split */ FETCH FIRST 2 ROWS WITH TIES /* trailing */;
SELECT k FROM r_ties ORDER BY k FETCH FIRST 2 ROWS WITH TIES /* split */ OFFSET 1 ROWS /* trailing */;
SELECT k FROM r_ties ORDER BY k DESC FETCH NEXT 2 ROWS WITH TIES;
SELECT k FROM r_ties ORDER BY k FETCH FIRST 0 ROWS WITH TIES;
SELECT k FROM r_ties ORDER BY k OFFSET 10 FETCH FIRST 1 ROW WITH TIES;
SELECT i FROM r_pp ORDER BY i;
SELECT * FROM r_pp limit 2;

SELECT count(1) FROM r_pp;
SELECT count(*) FROM r_pp;
SELECT count(i) FROM r_pp;
SELECT count(i), count(1), count(*) FROM r_pp;

-- should be rejected in router
SELECT count(id), i FROM r_pp GROUP BY i;

DROP TABLE r_ties;
DROP TABLE r_pp;


/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;