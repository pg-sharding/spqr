\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kr3 FROM 0 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr2  FROM -10 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr1 FROM -20 ROUTE TO sh2 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ADD DEFAULT SHARD sh1;

CREATE DISTRIBUTED RELATION def_sh_xx DISTRIBUTION KEY id IN ds1;

\c regress


CREATE TABLE def_sh_xx(id INT);

INSERT INTO def_sh_xx (id) VALUES (-30);
INSERT INTO def_sh_xx (id) VALUES (-21);
INSERT INTO def_sh_xx (id) VALUES (-20);
INSERT INTO def_sh_xx (id) VALUES (-19);
INSERT INTO def_sh_xx (id) VALUES (-11);
INSERT INTO def_sh_xx (id) VALUES (-10);
INSERT INTO def_sh_xx (id) VALUES (-9);
INSERT INTO def_sh_xx (id) VALUES (0);
INSERT INTO def_sh_xx (id) VALUES (1);

SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh4 */;

TRUNCATE def_sh_xx;

COPY def_sh_xx (id) FROM STDIN;
-30
-21
-20
-19
-11
-10
-9
0
1
\.

SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh4 */;

DROP TABLE def_sh_xx;


\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
