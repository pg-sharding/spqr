\c regress
SET __spqr__distribution = ds1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;