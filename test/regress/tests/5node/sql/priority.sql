-- sh1: primary 6433, replicas 6434..6437 (router-5node.yaml); 6434 sits in the
-- local AZ, so it is deterministically preferred by default; ALTER SHARD sets
-- replica 6437 to PRIORITY -1 (disabled), so it is only used as a last resort
\c spqr-console
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
CREATE RELATION pr_test (i);
\c regress
CREATE TABLE pr_test (i int);
INSERT INTO pr_test (i) VALUES (22);
SELECT count(*) FROM pr_test;
-- warm up prefer-standby on each replica
SET __spqr__reply_notice TO true;
SET __spqr__notice_message_format = '{shard}@{host}';
SET __spqr__execute_on TO sh1;
SET __spqr__target_session_attrs TO 'prefer-standby';
SET __spqr__execute_host_filter TO ':6434';
SELECT 1+2;
RESET __spqr__execute_host_filter;
SET __spqr__execute_host_filter TO ':6435';
SELECT 1+2;
RESET __spqr__execute_host_filter;
SET __spqr__execute_host_filter TO ':6436';
SELECT 1+2;
RESET __spqr__execute_host_filter;
SET __spqr__execute_host_filter TO ':6437';
SELECT 1+2;
RESET __spqr__execute_host_filter;
-- baseline: 6434 (local AZ) is preferred by default
SELECT 1+2;
-- disable replica 6437: the disabled host is only used as a last resort
\c spqr-console
ALTER SHARD sh1 OPTIONS (SET HOST '127.0.0.1:6437:far PRIORITY -1');
\c regress
-- cut the enabled replicas: only the primary and the disabled replica are left
\! iptables -A INPUT -p tcp --dport 6434 -j REJECT
\! iptables -A INPUT -p tcp --dport 6435 -j REJECT
\! iptables -A INPUT -p tcp --dport 6436 -j REJECT
\! sleep 2
SELECT 1+2;
\! iptables -D INPUT -p tcp --dport 6434 -j REJECT
\! iptables -D INPUT -p tcp --dport 6435 -j REJECT
\! iptables -D INPUT -p tcp --dport 6436 -j REJECT
-- restore the replica priority
\c spqr-console
ALTER SHARD sh1 OPTIONS (SET HOST '127.0.0.1:6437:far');
\c regress
RESET __spqr__target_session_attrs;
RESET __spqr__notice_message_format;
RESET __spqr__execute_on;
SET __spqr__reply_notice TO false;
-- CREATE TABLE writes a spqr_metadata row; DROP TABLE does not scrub it
DROP TABLE pr_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;