
\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION xjoin DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds1 ATTACH RELATION yjoin DISTRIBUTION KEY w_id;

CREATE DISTRIBUTION ds2 COLUMN TYPES varchar hash;
CREATE KEY RANGE krid2 FROM 3505849917 ROUTE TO sh2 FOR DISTRIBUTION ds2;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds2;
ALTER DISTRIBUTION ds2 ATTACH RELATION users DISTRIBUTION KEY user_id HASH FUNCTION MURMUR;
CREATE REFERENCE TABLE dict;

\c regress

SET __spqr__engine_v2 TO on;

CREATE TABLE xjoin(id int);
CREATE TABLE yjoin(w_id int);

INSERT INTO xjoin (id) values(1);
INSERT INTO xjoin (id) values(10);
INSERT INTO xjoin (id) values(15);
INSERT INTO xjoin (id) values(25);

INSERT INTO yjoin (w_id) values(1);
INSERT INTO yjoin (w_id) values(10);
INSERT INTO yjoin (w_id) values(15);
INSERT INTO yjoin (w_id) values(25);

--- XXX: fix
--SELECT * FROM xjoin JOIN yjoin on id=w_id ORDER BY id;
-- result is not full
--SELECT * FROM xjoin JOIN yjoin on true ORDER BY id;

SELECT * FROM xjoin JOIN yjoin on id=w_id where yjoin.w_id = 15 ORDER BY id;
-- XXX: this used to work by miracle. We should re-support this in engine v2
SELECT * FROM xjoin JOIN yjoin on id=w_id where w_id = 15 ORDER BY id /* __spqr__engine_v2: false */;
-- Join condition is distribution key, scatter out
SELECT * FROM xjoin JOIN yjoin on id=w_id ORDER BY id /* __spqr__engine_v2: false  */;

DROP TABLE xjoin;
DROP TABLE yjoin;
-- next test
create table users (user_id text, name text, dict_id int);
create table dict (dict_id int);

INSERT INTO users(user_id, name, dict_id) 
VALUES ('user1', 'name1', 1), ('user2', 'name2', 1), 
('user1', 'test', 1), ('user2', 'test', 1) /*__spqr__engine_v2: true*/;

SELECT u.user_id, u.name, d.dict_id
FROM users u
LEFT OUTER JOIN dict d ON u.dict_id = d.dict_id
WHERE u.name = 'test' /*__spqr__engine_v2: true*/;

SELECT u.user_id, u.name, d.dict_id
FROM users u
LEFT OUTER JOIN dict d ON u.dict_id = d.dict_id
order by u.name /*__spqr__engine_v2: true*/;

DROP TABLE users;
DROP TABLE dict;
RESET __spqr__engine_v2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
