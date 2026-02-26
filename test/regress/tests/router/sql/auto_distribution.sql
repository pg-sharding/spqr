\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;

CREATE KEY RANGE FROM 0 ROUTE TO sh1;

\c regress

-- should fail
CREATE TABLE zz(i int, j int, k int);

CREATE TABLE zz(i int, j int, k int) /* __spqr__auto_distribution: REPLICATED */;

INSERT INTO zz (i,j,k) VALUES(1,2,3);
INSERT INTO zz (i,j,k) VALUES(2,3,4);
INSERT INTO zz (i,j,k) VALUES(4,5,6);


TABLE zz /* __spqr__execute_on: sh1 */;
TABLE zz /* __spqr__execute_on: sh2 */;
TABLE zz /* __spqr__execute_on: sh3 */;
TABLE zz /* __spqr__execute_on: sh4 */;

-- should fail
CREATE TABLE d_zz (i int, j int);

-- should succeed .
CREATE TABLE d_zz (i int, j int) /* __spqr__auto_distribution: ds1, __spqr__distribution_key: j */;

-- should failx.
CREATE TABLE d_zz (i int, j int) /* __spqr__auto_distribution: ds1, __spqr__distribution_key: j */;

-- should succeed .
CREATE TABLE IF NOT EXISTS d_zz (i int, j int) /* __spqr__auto_distribution: ds1, __spqr__distribution_key: j */;

INSERT INTO d_zz (i, j) VALUES(1,2); 
INSERT INTO d_zz (i, j) VALUES(2,3); 

TABLE d_zz /* __spqr__execute_on: sh1 */;
TABLE d_zz /* __spqr__execute_on: sh2 */;
TABLE d_zz /* __spqr__execute_on: sh3 */;
TABLE d_zz /* __spqr__execute_on: sh4 */;

DROP TABLE zz;
DROP TABLE d_zz;

\c spqr-console

DROP DISTRIBUTION ALL CASCADE;
