-- a single shard sh1 with 1 primary (127.0.0.1:6433) and 4 replicas
-- (127.0.0.1:6434..6437); target-session-attrs decides which node answers
\c spqr-console
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
CREATE RELATION tsa_test (i);
\c regress
CREATE TABLE tsa_test (i int);
INSERT INTO tsa_test (i) VALUES (22);
SELECT count(*) FROM tsa_test;
-- reply_notice is off by default; flip it on for pinned queries to see which node answered
SET __spqr__reply_notice TO true;
SET __spqr__notice_message_format = '{shard}@{host}';
SET __spqr__execute_on TO sh1;
SET __spqr__execute_host_filter TO ':6433';
SET __spqr__target_session_attrs TO 'read-write';
SELECT 1+2;
RESET __spqr__target_session_attrs;
RESET __spqr__execute_host_filter;
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
RESET __spqr__target_session_attrs;
RESET __spqr__notice_message_format;
RESET __spqr__execute_on;
SET __spqr__reply_notice TO false;
SELECT __spqr__host_status('127.0.0.1:6433');
SELECT __spqr__host_status('127.0.0.1:6434');
SELECT __spqr__host_status('127.0.0.1:6435');
SELECT __spqr__host_status('127.0.0.1:6436');
SELECT __spqr__host_status('127.0.0.1:6437');
-- the router writes distributed-relation metadata into the shard's spqr_metadata
-- on CREATE TABLE; DROP TABLE does not scrub that row, so remove it explicitly
DROP TABLE tsa_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;