UNREGISTER ROUTER ALL;

REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

DROP KEY RANGE krid1;

CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

CREATE KEY RANGE krid2 FROM 33 ROUTE TO nonexistentshard FOR DISTRIBUTION ds1;

SHOW key_ranges_extended(created_at, updated_at) WHERE key_range_id = 'krid2' AND created_at <> 'N/A' \gset
\c - - regress_router 7432
SHOW LOCAL key_ranges_extended(key_range_id) WHERE key_range_id = 'krid2' AND created_at = :'created_at' AND updated_at = :'updated_at';
\c - - regress_coordinator 7002

LOCK KEY RANGE krid2;
MOVE KEY RANGE krid2 TO sh2 META ONLY;
UNLOCK KEY RANGE krid2;
SHOW key_ranges_extended(updated_at)
WHERE key_range_id = 'krid2' AND shard_id = 'sh2' AND created_at = :'created_at' AND updated_at <> 'N/A' \gset moved_
\c - - regress_router 7432
SHOW LOCAL key_ranges_extended(key_range_id, shard_id)
WHERE key_range_id = 'krid2' AND created_at = :'created_at' AND updated_at = :'moved_updated_at';
\c - - regress_coordinator 7002

-- XXX: fix router meta deploy here
UNREGISTER ROUTER ALL;

LOCK KEY RANGE krid2;

DROP KEY RANGE ALL;

CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;

SHOW key_ranges(key_range_id, shard_id, distribution_id, lower_bound, locked);

LOCK KEY RANGE krid2;
UNLOCK KEY RANGE krid2;

DROP KEY RANGE ALL;

REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";

DROP DISTRIBUTION ALL CASCADE;
UNREGISTER ROUTER ALL;
