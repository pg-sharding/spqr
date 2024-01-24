\c spqr-console
DROP DATASPACE ALL CASCADE;

CREATE DATASPACE ds1;
CREATE DATASPACE ds2;

CREATE SHARDING RULE r1 COLUMNS w_id FOR DATASPACE ds1;
CREATE SHARDING RULE r2 COLUMNS w_id FOR DATASPACE ds2;

CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DATASPACE ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DATASPACE ds1;

CREATE KEY RANGE krid3 FROM 11 ROUTE TO sh2 FOR DATASPACE ds2;

\c regress

SET __spqr__dataspace = ds1;

CREATE TABLE xx (w_id int);

INSERT INTO xx(w_id) VALUES(5);

INSERT INTO xx(w_id) VALUES(20);

SELECT * FROM xx WHERE w_id=5;

SET __spqr__dataspace = ds2;

SELECT * FROM xx WHERE w_id=5;

\c spqr-console
DROP DATASPACE ALL CASCADE
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;