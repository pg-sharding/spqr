-- Run last: promoting replica 6434 changes the five-node topology.
\c spqr-console
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
CREATE RELATION tsa_promote_test (i);
\c regress
SET __spqr__execute_on TO sh1;
SET __spqr__target_session_attrs TO 'read-write';
SET __spqr__execute_host_filter TO ':6433';
CREATE TABLE tsa_promote_test (i int);
INSERT INTO tsa_promote_test VALUES (22);
SELECT inet_server_port(), pg_is_in_recovery(), count(*) FROM tsa_promote_test;

-- A standby cannot serve a read-write request.
SET __spqr__execute_host_filter TO ':6434';
SELECT count(*) FROM tsa_promote_test;

-- Make replica 6435 unavailable and promote replica 6434.
\! iptables -A INPUT -p tcp --dport 6435 -j REJECT
\! bash /regress/tests/5node/sql/promote.sh

-- The same read-write route must now accept the promoted host.
SELECT inet_server_port(), pg_is_in_recovery(), count(*) FROM tsa_promote_test;
INSERT INTO tsa_promote_test VALUES (23);
SELECT inet_server_port(), pg_is_in_recovery(), count(*) FROM tsa_promote_test;

-- The unavailable replica must fail; the other replicas still serve reads.
SET __spqr__target_session_attrs TO 'read-only';
SET __spqr__execute_host_filter TO ':6435';
SELECT count(*) FROM tsa_promote_test;
SET __spqr__execute_host_filter TO ':6436';
SELECT inet_server_port(), pg_is_in_recovery(), count(*) FROM tsa_promote_test;
SET __spqr__execute_host_filter TO ':6437';
SELECT inet_server_port(), pg_is_in_recovery(), count(*) FROM tsa_promote_test;
\! iptables -D INPUT -p tcp --dport 6435 -j REJECT

-- Clean up both timelines.
SET __spqr__target_session_attrs TO 'read-write';
SET __spqr__execute_host_filter TO ':6434';
DROP TABLE tsa_promote_test;
DELETE FROM spqr_metadata.spqr_distributed_relations;
SET __spqr__execute_host_filter TO ':6433';
DROP TABLE tsa_promote_test;
DELETE FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
