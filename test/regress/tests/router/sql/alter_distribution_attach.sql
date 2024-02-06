\c spqr-console

CREATE DISTRIBUTION ds1;
CREATE DISTRIBUTION ds2;

CREATE SHARDING RULE r1 COLUMNS w_id FOR DISTRIBUTION ds1;
CREATE SHARDING RULE r2 COLUMNS w_id FOR DISTRIBUTION ds2;

CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds2;

CREATE KEY RANGE FROM 1 ROUTE TO sh2 FOR DISTRIBUTION ds2;

ALTER DISTRIBUTION ds1 ATTACH RELATION xx COLUMNS w_id;
ALTER DISTRIBUTION ds2 ATTACH RELATION yy COLUMNS w_id;
ALTER DISTRIBUTION ds3 ATTACH RELATION xx COLUMNS w_id;

\c regress

DROP TABLE IF EXISTS xx;

CREATE TABLE xx (w_id int);

INSERT INTO xx(w_id) VALUES(5);

INSERT INTO xx(w_id) VALUES(20);

SELECT * FROM xx WHERE w_id=5;

CREATE TABLE yy (w_id int);

SELECT * FROM yy WHERE w_id=5;

\c spqr-console

ALTER DISTRIBUTION ds1 ATTACH RELATION yy COLUMNS w_id;

\c regress

SELECT * FROM yy WHERE w_id=5;

SET __spqr__distribution = ds2;

SELECT * FROM xx WHERE w_id=5;

DROP TABLE xx;
DROP TABLE yy;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;