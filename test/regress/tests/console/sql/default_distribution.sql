CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;

CREATE DISTRIBUTED RELATION r1 (id);

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

CREATE DISTRIBUTION ds2 COLUMN TYPES integer;
CREATE KEY RANGE krid3 FROM 11 ROUTE TO sh2;

CREATE DISTRIBUTED RELATION r1 (id);

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);


DROP DISTRIBUTION ALL CASCADE;