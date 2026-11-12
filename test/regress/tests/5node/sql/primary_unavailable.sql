-- A failed primary connection should be reported alongside every replica.
\c spqr-console
CREATE DISTRIBUTION host_error_d (int);
CREATE KEY RANGE host_error_kr FROM 0 ROUTE TO sh1;
CREATE RELATION host_error_test (i);
\c regress
\! psql -h 127.0.0.1 -p 6433 -U postgres -d regress -c 'CREATE TABLE host_error_test (i int)' >/dev/null
\c spqr-console
INVALIDATE BACKENDS;
\c regress
\set VERBOSITY sqlstate
SET __spqr__execute_on TO sh1;
SET __spqr__target_session_attrs TO 'read-write';
\! iptables -A INPUT -p tcp --dport 6433 -j REJECT
\! sleep 1
SELECT count(*) FROM host_error_test;
\! iptables -D INPUT -p tcp --dport 6433 -j REJECT
RESET __spqr__target_session_attrs;
RESET __spqr__execute_on;
SELECT
    :'LAST_ERROR_MESSAGE' LIKE '%host 127.0.0.1:6433:%connection refused%' AS primary_reported,
    :'LAST_ERROR_MESSAGE' LIKE '%host 127.0.0.1:6434:%' AS replica_1_reported,
    :'LAST_ERROR_MESSAGE' LIKE '%host 127.0.0.1:6435:%' AS replica_2_reported,
    :'LAST_ERROR_MESSAGE' LIKE '%host 127.0.0.1:6436:%' AS replica_3_reported,
    :'LAST_ERROR_MESSAGE' LIKE '%host 127.0.0.1:6437:%' AS replica_4_reported;
DROP TABLE host_error_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
