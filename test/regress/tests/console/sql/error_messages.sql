\set VERBOSITY verbose
\pset format unaligned

DROP DISTRIBUTION missing_error_ds;
DROP UNIQUE INDEX missing_error_idx;
CREATE DISTRIBUTION error_ds COLUMN TYPES integer DEFAULT SHARD missing_error_shard;

CREATE DISTRIBUTION error_ds COLUMN TYPES integer;
CREATE DISTRIBUTION error_ds COLUMN TYPES integer;
ALTER DISTRIBUTION error_ds DETACH RELATION missing_error_relation;

DROP DISTRIBUTION IF EXISTS missing_error_ds;
CREATE DISTRIBUTION IF NOT EXISTS error_ds COLUMN TYPES integer;
DROP DISTRIBUTION error_ds;

\set VERBOSITY default
