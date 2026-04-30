
\c spqr-console
CREATE DISTRIBUTION ds1 (integer HASH);
CREATE KEY RANGE kridi2 from 2147483648 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
CREATE RELATION xjoin (id HASH murmur);
CREATE RELATION yjoin (w_id HASH murmur);
CREATE RELATION zjoin (a HASH murmur);

\c regress

SET __spqr__engine_v2 TO on;

CREATE TABLE xjoin(id int);
CREATE TABLE yjoin(w_id int);
CREATE TABLE zjoin(a int, b int);

INSERT INTO xjoin (id) values(1);
INSERT INTO xjoin (id) values(10);
INSERT INTO xjoin (id) values(15);
INSERT INTO xjoin (id) values(25);

INSERT INTO yjoin (w_id) values(1);
INSERT INTO yjoin (w_id) values(10);
INSERT INTO yjoin (w_id) values(15);
INSERT INTO yjoin (w_id) values(25);

-- result is not full
--SELECT * FROM xjoin JOIN yjoin on true ORDER BY id;

SELECT * FROM xjoin JOIN yjoin on id=w_id where yjoin.w_id = 15 ORDER BY id;

SELECT * FROM xjoin JOIN yjoin on id=w_id where yjoin.w_id = 15 and xjoin.id = 15  ORDER BY id;

SELECT * FROM xjoin JOIN yjoin on id=w_id where yjoin.w_id = 15 and xjoin.id = 1  ORDER BY id;


-- XXX: this used to work by miracle. We should re-support this in engine v2
SELECT * FROM xjoin JOIN yjoin on id=w_id where w_id = 15 ORDER BY id /* __spqr__engine_v2: false */;
-- Join condition is distribution key, scatter out
SELECT * FROM xjoin JOIN yjoin on id=w_id ORDER BY id /* __spqr__engine_v2: false  */;

-- self join
SELECT FROM xjoin a JOIN xjoin b ON true;

-- routable self join
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 12;
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 13;
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 14;
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 15;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE WHERE b = 1;

-- catalog JOIN routing. XXX: make it deterministic and support.
-- SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore';


--with v (i) as (values(10), (20), (25)) select * from v LEFT join xjoin t on v.i = t.id;
--with v (i) as (values(10), (20), (25)) select * from v RIGHT join xjoin t on v.i = t.id;
with v (i) as (values(10), (20), (25)) select * from v join xjoin t on v.i = t.id;

-- no multishard/rewrite
with v (j, i) as (values(1, 12), (1, 13), (1, 15)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 12), (1, 13), (1, 15)), v2 (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 12), (1, 13), (1, 15)) select * from v join xjoin t on v.i = t.id WHERE v.j <= t.id;

-- with multishard/rewrite
with v (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 20), (1, 25)), v2 (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id WHERE v.j <= t.id;

with v (i) as (values(10), (20), (25)) select * from v LEFT join xjoin t on t.id = v.i;
with v (i) as (values(10), (20), (25)) select * from v RIGHT join xjoin t on t.id = v.i;
with v (i) as (values(10), (20), (25)) select * from v join xjoin t on t.id = v.i;

with v (i) as (values(10), (20), (25)), z as (select * from v join xjoin t on v.i = t.id) SELECT * FROM z;

DROP TABLE xjoin;
DROP TABLE yjoin;
DROP TABLE zjoin;

RESET __spqr__engine_v2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
