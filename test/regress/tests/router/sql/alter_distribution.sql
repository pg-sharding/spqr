\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES int;
CREATE DISTRIBUTION ds2 COLUMN TYPES int;

CREATE KEY RANGE krid3 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds2;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 1 ROUTE TO sh2 FOR DISTRIBUTION ds2;

ALTER DISTRIBUTION ds1 ATTACH RELATION xx DISTRIBUTION KEY w_id;
ALTER DISTRIBUTION ds2 ATTACH RELATION yy DISTRIBUTION KEY w_id;
ALTER DISTRIBUTION ds3 ATTACH RELATION xx DISTRIBUTION KEY w_id;
ALTER DISTRIBUTION ds1 ATTACH RELATION xx DISTRIBUTION KEY w_id;

\c regress

DROP TABLE IF EXISTS xx;

CREATE TABLE xx (w_id int);

INSERT INTO xx(w_id) VALUES(5);

INSERT INTO xx(w_id) VALUES(20);

SELECT * FROM xx WHERE w_id=5;

CREATE TABLE yy (w_id int);

SELECT * FROM yy WHERE w_id=5;

\c spqr-console

ALTER DISTRIBUTION ds1 ATTACH RELATION yy DISTRIBUTION KEY w_id;
ALTER DISTRIBUTION ds2 DETACH RELATION yy;
ALTER DISTRIBUTION ds2 DETACH RELATION yy;
ALTER DISTRIBUTION ds1 ATTACH RELATION yy DISTRIBUTION KEY w_id;

\c regress

SELECT * FROM yy WHERE w_id=5;

DROP TABLE xx;
DROP TABLE yy;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

