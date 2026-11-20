\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kr1 FROM 100 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr2 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

BEGIN;

CREATE RELATION test (id) IN ds1;
RENAME KEY RANGE kr1 TO kr1_new;

COMMIT;

SHOW DISTRIBUTIONS(distribution_id, column_types, default_shard) ORDER BY distribution_id;
SHOW RELATIONS(relation_name, distribution_id, distribution_key) ORDER BY relation_name;
SHOW KEY_RANGES(key_range_id, shard_id, distribution_id, lower_bound, locked) ORDER BY key_range_id;

BEGIN;

DROP DISTRIBUTION ALL CASCADE;

ROLLBACK;

SHOW DISTRIBUTIONS(distribution_id, column_types, default_shard) ORDER BY distribution_id;
SHOW RELATIONS(relation_name, distribution_id, distribution_key) ORDER BY relation_name;
SHOW KEY_RANGES(key_range_id, shard_id, distribution_id, lower_bound, locked) ORDER BY key_range_id;

DROP DISTRIBUTION ALL CASCADE;
