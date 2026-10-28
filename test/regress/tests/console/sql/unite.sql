CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid5 FROM 60 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid4 FROM 40 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 20 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;

\set VERBOSITY terse

-- non adjacent key ranges
UNITE KEY RANGE krid1 WITH krid3;

UNITE KEY RANGE krid1 WITH krid2;
SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

-- locked key range
LOCK KEY RANGE krid1;
UNITE KEY RANGE krid1 WITH krid3;
UNLOCK KEY RANGE krid1;

LOCK KEY RANGE krid5;
UNITE KEY RANGE krid4 WITH krid5;
UNLOCK KEY RANGE krid5;

-- reverse order
UNITE KEY RANGE krid3 WITH krid1;
SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

-- routing to different shards
UNITE KEY RANGE krid3 WITH krid4;

DROP DISTRIBUTION ALL CASCADE;
