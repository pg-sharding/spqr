CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

-- create key range works
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

-- intersecting key range
CREATE KEY RANGE krid3 FROM 50 ROUTE TO sh1 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

DROP KEY RANGE ALL;

-- intersecting key range
CREATE KEY RANGE krid3 FROM -50 ROUTE TO sh1 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

DROP DISTRIBUTION ALL CASCADE;
