
\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
CREATE RELATION xjoin (id);
CREATE RELATION yjoin (w_id);
CREATE RELATION zjoin (a);

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
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 15;
SELECT FROM xjoin a JOIN xjoin b ON true WHERE a.id = 11;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE;

SELECT FROM xjoin JOIN yjoin ON TRUE JOIN zjoin ON TRUE WHERE b = 1;

-- catalog JOIN rouring.
SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore';

DROP TABLE xjoin;
DROP TABLE yjoin;
DROP TABLE zjoin;

RESET __spqr__engine_v2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
