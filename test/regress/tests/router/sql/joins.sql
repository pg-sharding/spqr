
\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
CREATE RELATION xjoin (id);
CREATE RELATION xjoin2 (id);
CREATE RELATION yjoin (w_id);
CREATE RELATION zjoin (a);

CREATE REFERENCE RELATION rf;

\c regress

SET __spqr__engine_v2 TO on;

CREATE TABLE xjoin(id int);
CREATE TABLE xjoin2(id int);
CREATE TABLE yjoin(w_id int);
CREATE TABLE zjoin(a int, b int);

CREATE TABLE rf(rid INT);

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
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 15;
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 11;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE WHERE b = 1;

-- catalog JOIN routing. XXX: make it deterministic and support.
-- SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore';


--with v (i) as (values(10), (20), (25)) select * from v LEFT join xjoin t on v.i = t.id;
--with v (i) as (values(10), (20), (25)) select * from v RIGHT join xjoin t on v.i = t.id;
with v (i) as (values(10), (20), (25)) select * from v join xjoin t on v.i = t.id;

-- no multishard/rewrite
with v (j, i) as (values(1, 10), (1, 9), (1, 8)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 9), (1, 8)), v2 (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 9), (1, 8)) select * from v join xjoin t on v.i = t.id WHERE v.j <= t.id;

-- with multishard/rewrite
with v (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 20), (1, 25)), v2 (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id;
with v (j, i) as (values(1, 10), (1, 20), (1, 25)) select * from v join xjoin t on v.i = t.id WHERE v.j <= t.id;


with v (i) as (values(10), (20), (25)) select * from v LEFT join xjoin t on t.id = v.i;
with v (i) as (values(10), (20), (25)) select * from v RIGHT join xjoin t on t.id = v.i;
with v (i) as (values(10), (20), (25)) select * from v join xjoin t on t.id = v.i;

with v (i) as (values(10), (20), (25)), z as (select * from v join xjoin t on v.i = t.id) SELECT * FROM z;

with v(j, a) as (values(1,10)), v_j_d as (select * from v join rf on id=rid) , c as (select xx.id from xjoin xx join v_j_d  vv on vv.id=xx.id) select * from xjoin2 yy join c zz on zz.id=yy.id;

DROP TABLE xjoin;
DROP TABLE xjoin2;
DROP TABLE yjoin;
DROP TABLE zjoin;

DROP TABLE rf;

RESET __spqr__engine_v2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
