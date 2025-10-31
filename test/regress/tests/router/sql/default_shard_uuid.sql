\c spqr-console

-- check that UUID type works with default shard
CREATE DISTRIBUTION ds2 COLUMN TYPES uuid;
CREATE KEY RANGE kr2 FROM '88888888-8888-8888-8888-888888888888' ROUTE TO sh2 FOR DISTRIBUTION ds2;
CREATE KEY RANGE kr1 FROM '00000000-0000-0000-0000-000000000001' ROUTE TO sh1 FOR DISTRIBUTION ds2;

ALTER DISTRIBUTION ds2 ADD DEFAULT SHARD sh1;

CREATE DISTRIBUTED RELATION def_sh_uuid DISTRIBUTION KEY id IN ds2;

\c regress

CREATE TABLE def_sh_uuid(id UUID);

-- Insert values that should route to default shard (less than first key range)
INSERT INTO def_sh_uuid (id) VALUES ('00000000-0000-0000-0000-000000000000');
-- Insert values that should route to regular key ranges
INSERT INTO def_sh_uuid (id) VALUES ('00000000-0000-0000-0000-000000000001');
INSERT INTO def_sh_uuid (id) VALUES ('88888888-8888-8888-8888-888888888888');
INSERT INTO def_sh_uuid (id) VALUES ('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF');

SELECT * FROM def_sh_uuid ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_uuid ORDER BY id /* __spqr__execute_on: sh2 */;

DROP TABLE def_sh_uuid;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

