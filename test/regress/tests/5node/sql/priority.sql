-- a single shard sh1 with 1 primary (127.0.0.1:6433) and 4 replicas
-- (127.0.0.1:6434..6437); a driver for the host-priority test: cut off
-- everything but the primary and one replica, then check that prefer-standby
-- still routes to the surviving replica (once that replica is disabled with
-- PRIORITY -1 via ALTER SHARD, the same query must fall back to the primary)
\c spqr-console
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
CREATE RELATION pr_test (i);
\c regress
CREATE TABLE pr_test (i int);
INSERT INTO pr_test (i) VALUES (22);
SELECT count(*) FROM pr_test;
-- warm up the prefer-standby route on each replica so all of them are usable
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
-- cut off everything except the primary (6433) and one replica (6436)
\! iptables -A INPUT -p tcp --dport 6434 -j REJECT
\! iptables -A INPUT -p tcp --dport 6435 -j REJECT
\! iptables -A INPUT -p tcp --dport 6437 -j REJECT
\! sleep 2
-- prefer-standby still routes to the only reachable replica
SELECT 1+2;
-- TODO(priority): ALTER SHARD sh1 ... SET HOST '127.0.0.1:6436 PRIORITY -1';
-- with the last replica disabled, the same query must fall back to the primary
\! iptables -D INPUT -p tcp --dport 6434 -j REJECT
\! iptables -D INPUT -p tcp --dport 6435 -j REJECT
\! iptables -D INPUT -p tcp --dport 6437 -j REJECT
RESET __spqr__target_session_attrs;
RESET __spqr__notice_message_format;
RESET __spqr__execute_on;
SET __spqr__reply_notice TO false;
-- the router writes distributed-relation metadata into the shard's spqr_metadata
-- on CREATE TABLE; DROP TABLE does not scrub that row, so remove it explicitly
DROP TABLE pr_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;