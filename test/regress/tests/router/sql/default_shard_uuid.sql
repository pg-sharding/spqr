\c spqr-console

-- check that UUID type works
CREATE DISTRIBUTION ds1 COLUMN TYPES UUID;
CREATE KEY RANGE krid2 FROM '88888888-8888-8888-8888-888888888888' ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM '00000000-0000-0000-0000-000000000001' ROUTE TO sh1 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ADD DEFAULT SHARD sh1;

CREATE DISTRIBUTED RELATION def_sh_xx DISTRIBUTION KEY id IN ds1;

\c regress


CREATE TABLE def_sh_xx(id UUID);

INSERT INTO def_sh_xx (id) VALUES ('00000000-0000-0000-0000-000000000000');
INSERT INTO def_sh_xx (id) VALUES ('00000000-0000-0EA0-0FAB-AB031389FF09');
INSERT INTO def_sh_xx (id) VALUES ('fa98123B-0000-0Ea0-0fab-ab031389FF09');

SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh4 */;

TRUNCATE def_sh_xx;

COPY def_sh_xx (id) FROM STDIN;
00000000-0000-0000-0000-000000000000
00000000-0000-0EA0-0FAB-AB031389FF09
fa98123B-0000-0Ea0-0fab-ab031389FF09
\.

SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM def_sh_xx ORDER BY id /* __spqr__execute_on: sh4 */;

DROP TABLE def_sh_xx;


\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
