CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

DROP KEY RANGE krid1;

CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

CREATE KEY RANGE krid2 FROM 33 ROUTE TO nonexistentshard FOR DISTRIBUTION ds1;

DROP DISTRIBUTION ALL CASCADE;

-- IF NOT EXISTS keeps CREATE and ATTACH commands idempotent.
CREATE DISTRIBUTION IF NOT EXISTS idempotent_ds COLUMN TYPES integer;
CREATE DISTRIBUTION IF NOT EXISTS idempotent_ds COLUMN TYPES varchar;

ALTER DISTRIBUTION idempotent_ds ATTACH RELATION IF NOT EXISTS existing_rel DISTRIBUTION KEY id;
ALTER DISTRIBUTION idempotent_ds ATTACH RELATION IF NOT EXISTS existing_rel DISTRIBUTION KEY id
    RELATION new_rel DISTRIBUTION KEY id;

-- IF NOT EXISTS does not suppress errors unrelated to relation existence.
ALTER DISTRIBUTION missing_ds ATTACH RELATION IF NOT EXISTS orphan_rel DISTRIBUTION KEY id;

CREATE KEY RANGE IF NOT EXISTS idempotent_kr FROM 0 ROUTE TO sh1 FOR DISTRIBUTION idempotent_ds;
CREATE KEY RANGE IF NOT EXISTS idempotent_kr FROM 100 ROUTE TO sh2 FOR DISTRIBUTION idempotent_ds;

CREATE REFERENCE TABLE IF NOT EXISTS idempotent_ref ON sh1;
CREATE REFERENCE TABLE IF NOT EXISTS idempotent_ref ON sh2;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);
SHOW relations WHERE distribution_id = 'idempotent_ds';
SHOW reference_relations;

DROP DISTRIBUTION ALL CASCADE;
