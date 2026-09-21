\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE k4 FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE k3 FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE k2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE k1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

\c regress

SELECT * FROM __spqr__show('key_ranges') WHERE shard_id = 'sh1';
SELECT * FROM __spqr__show('key_ranges') WHERE shard_id = 'sh2' OR shard_id = 'sh3';
SELECT key_range_id, shard_id, lower_bound FROM __spqr__show('key_ranges') WHERE lower_bound = '101';
SELECT * FROM __spqr__show('key_ranges') WHERE locked = 'false';
SELECT * FROM __spqr__show('key_ranges') WHERE shard_id <> 'sh1';
SELECT * FROM __spqr__show('key_ranges') WHERE shard_id <> 'sh1' AND shard_id <> 'sh4';
SELECT * FROM __spqr__show('key_ranges') WHERE shard_id != 'sh1';

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
