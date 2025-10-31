\c spqr-console

-- check that TIMESTAMPTZ type works with default shard
CREATE DISTRIBUTION ds3 COLUMN TYPES timestamptz;
CREATE KEY RANGE kr2 FROM '2024-01-01T00:00:00Z' ROUTE TO sh2 FOR DISTRIBUTION ds3;
CREATE KEY RANGE kr1 FROM '2023-01-01T00:00:00Z' ROUTE TO sh1 FOR DISTRIBUTION ds3;

ALTER DISTRIBUTION ds3 ADD DEFAULT SHARD sh1;

CREATE DISTRIBUTED RELATION def_sh_timestamptz DISTRIBUTION KEY id IN ds3;

\c regress

CREATE TABLE def_sh_timestamptz(id TIMESTAMPTZ);

-- Insert values that should route to default shard (less than first key range)
INSERT INTO def_sh_timestamptz (id) VALUES ('2022-01-01T00:00:00Z');
-- Insert values that should route to regular key ranges
INSERT INTO def_sh_timestamptz (id) VALUES ('2023-01-01T00:00:00Z');
INSERT INTO def_sh_timestamptz (id) VALUES ('2024-01-01T00:00:00Z');
INSERT INTO def_sh_timestamptz (id) VALUES ('2025-01-01T00:00:00Z');

SELECT * FROM def_sh_timestamptz ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM def_sh_timestamptz ORDER BY id /* __spqr__execute_on: sh2 */;

DROP TABLE def_sh_timestamptz;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

