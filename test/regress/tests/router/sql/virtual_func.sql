
\c spqr-console

CREATE DISTRIBUTION d1 COLUMN TYPES integer;
CREATE KEY RANGE FROM 300 ROUTE TO sh4 FOR DISTRIBUTION d1;
CREATE KEY RANGE FROM 200 ROUTE TO sh3 FOR DISTRIBUTION d1;
CREATE KEY RANGE FROM 100 ROUTE TO sh2 FOR DISTRIBUTION d1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d1;


-- check that numeric type works
CREATE DISTRIBUTION d2 (integer HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION d2;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION d2;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION d2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d2;

-- XXX: hacky hack, fix that
CREATE DISTRIBUTED RELATION xx_insert_rel_hash(a HASH MURMUR) IN d2;


-- check that varchar type works
CREATE DISTRIBUTION d3 (varchar HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION d3;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION d3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION d3;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d3;

-- XXX: hacky hack, fix that
CREATE DISTRIBUTED RELATION xx_insert_rel_hash_text (a HASH MURMUR) IN d3;

\c regress

-- non-hashed

select __spqr__route_key('d1', '1');
select __spqr__route_key('d1', '2');
select __spqr__route_key('d1', '3');
select __spqr__route_key('d1', '50');
select __spqr__route_key('d1', '99');
select __spqr__route_key('d1', '100');
select __spqr__route_key('d1', '101');
select __spqr__route_key('d1', '111');
select __spqr__route_key('d1', '121');
select __spqr__route_key('d1', '199');
select __spqr__route_key('d1', '200');
select __spqr__route_key('d1', '201');
select __spqr__route_key('d1', '299');
select __spqr__route_key('d1', '300');
select __spqr__route_key('d1', '301');

-- hashed
select __spqr__route_key('d2', '1');
select __spqr__route_key('d2', '2');
select __spqr__route_key('d2', '3');
select __spqr__route_key('d2', '50');
select __spqr__route_key('d2', '99');
select __spqr__route_key('d2', '100');
select __spqr__route_key('d2', '101');
select __spqr__route_key('d2', '111');
select __spqr__route_key('d2', '121');
select __spqr__route_key('d2', '199');
select __spqr__route_key('d2', '200');
select __spqr__route_key('d2', '201');
select __spqr__route_key('d2', '299');
select __spqr__route_key('d2', '300');
select __spqr__route_key('d2', '301');


-- hashed text
select __spqr__route_key('d3', 'a');
select __spqr__route_key('d3', 'b');
select __spqr__route_key('d3', 'c');
select __spqr__route_key('d3', 'd');
select __spqr__route_key('d3', 'e');
select __spqr__route_key('d3', 'f');
select __spqr__route_key('d3', 'g');
select __spqr__route_key('d3', 'h');
select __spqr__route_key('d3', 'i');
select __spqr__route_key('d3', 'j');
select __spqr__route_key('d3', 'k');
select __spqr__route_key('d3', 'l');
select __spqr__route_key('d3', 'm');
select __spqr__route_key('d3', 'n');
select __spqr__route_key('d3', 'o');

-- test __spqr__host_status: check rw status of each host in each shard
-- topology: sh1-sh4, each has a primary (spqr_shard_N) and a replica (spqr_shard_N_replica)
-- primaries are RW, replicas are RO

-- sh1: primary
SET __spqr__execute_on TO sh1;
SET __spqr__execute_host_filter TO 'spqr_shard_1';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_1:6432');

-- sh1: replica
SET __spqr__execute_host_filter TO 'spqr_shard_1_replica';
SET __spqr__target_session_attrs TO 'prefer-standby';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_1_replica:6432');
RESET __spqr__target_session_attrs;

-- sh2: primary
SET __spqr__execute_on TO sh2;
SET __spqr__execute_host_filter TO 'spqr_shard_2';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_2:6432');

-- sh2: replica
SET __spqr__execute_host_filter TO 'spqr_shard_2_replica';
SET __spqr__target_session_attrs TO 'prefer-standby';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_2_replica:6432');
RESET __spqr__target_session_attrs;

-- sh3: primary
SET __spqr__execute_on TO sh3;
SET __spqr__execute_host_filter TO 'spqr_shard_3';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_3:6432');

-- sh3: replica
SET __spqr__execute_host_filter TO 'spqr_shard_3_replica';
SET __spqr__target_session_attrs TO 'prefer-standby';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_3_replica:6432');
RESET __spqr__target_session_attrs;

-- sh4: primary
SET __spqr__execute_on TO sh4;
SET __spqr__execute_host_filter TO 'spqr_shard_4';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_4:6432');

-- sh4: replica
SET __spqr__execute_host_filter TO 'spqr_shard_4_replica';
SET __spqr__target_session_attrs TO 'prefer-standby';
SELECT 1+2;
SELECT __spqr__host_status('spqr_shard_4_replica:6432');
RESET __spqr__target_session_attrs;

-- cleanup GUCs
RESET __spqr__execute_on;
RESET __spqr__execute_host_filter;

-- error: unknown host
SELECT __spqr__host_status('nonexistent_host:6432');

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;