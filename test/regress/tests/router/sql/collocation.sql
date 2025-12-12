\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE DISTRIBUTION ds2 COLUMN TYPES integer;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE KEY RANGE krid11 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds2;
CREATE KEY RANGE krid12 FROM 20 ROUTE TO sh1 FOR DISTRIBUTION ds2;

ALTER DISTRIBUTION ds1 ATTACH RELATION xx1 DISTRIBUTION KEY i;
ALTER DISTRIBUTION ds2 ATTACH RELATION xx2 DISTRIBUTION KEY j;


CREATE REFERENCE TABLE reft;
CREATE REFERENCE TABLE reft2;

\c regress
CREATE TABLE xx1 (i int);
CREATE TABLE xx2 (j int);

CREATE TABLE reft(ii int);
CREATE TABLE reft2(jj int);

COPY reft FROM STDIN;
1
2
3
\.

COPY reft2 FROM STDIN;
10
20
30
\.

COPY xx1 (i) FROM STDIN /* __spqr__allow_multishard */;
5
10
15
20
25
\.

COPY xx2 (j) FROM STDIN /* __spqr__allow_multishard */;
5
10
15
20
25
\.

INSERT INTO xx1 TABLE reft; -- ok
INSERT INTO xx2 TABLE reft; -- ok

INSERT INTO xx1 SELECT a.ii + b.jj FROM reft a, reft2 b; --ok

-- Below is very quitionable
--INSERT INTO xx1 TABLE xx2; -- should fail, different distributions;
/* maybe test join here? */

--INSERT INTO xx1 SELECT a.ii + b.j FROM reft a, xx2 b; --similar

DROP TABLE xx1;
DROP TABLE xx2;

DROP TABLE reft;
DROP TABLE reft2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

