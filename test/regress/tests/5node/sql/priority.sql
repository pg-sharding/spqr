-- sh1: primary 6433, replicas 6434..6437;
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
SET __spqr__execute_host_filter TO ':6435';
SELECT 1+2;
SET __spqr__execute_host_filter TO ':6436';
SELECT 1+2;
SET __spqr__execute_host_filter TO ':6437';
SELECT 1+2;
RESET __spqr__execute_host_filter;

SELECT 1+2;

RESET __spqr__execute_on;
SELECT __spqr__console_execute('ALTER SHARD sh1 OPTIONS (SET HOST ''127.0.0.1:6437:far PRIORITY -1'')') /*__spqr__preferred_engine: v2 */;
SET __spqr__execute_on TO sh1;

-- cut network except 6437
\! iptables -A INPUT -p tcp --dport 6433 -j REJECT
\! iptables -A INPUT -p tcp --dport 6434 -j REJECT
\! iptables -A INPUT -p tcp --dport 6435 -j REJECT
\! iptables -A INPUT -p tcp --dport 6436 -j REJECT
\! sleep 2

-- fallback to disabled
SELECT 1+2;

\! iptables -D INPUT -p tcp --dport 6433 -j REJECT
\! iptables -D INPUT -p tcp --dport 6434 -j REJECT
\! iptables -D INPUT -p tcp --dport 6435 -j REJECT
\! iptables -D INPUT -p tcp --dport 6436 -j REJECT
RESET __spqr__execute_on;
SELECT __spqr__console_execute('ALTER SHARD sh1 OPTIONS (SET HOST ''127.0.0.1:6437:far'')') /*__spqr__preferred_engine: v2 */;
SET __spqr__execute_on TO sh1;
-- cut network except 6434 and 6435
\! iptables -A INPUT -p tcp --dport 6433 -j REJECT
\! iptables -A INPUT -p tcp --dport 6436 -j REJECT
\! iptables -A INPUT -p tcp --dport 6437 -j REJECT
\! sleep 2
-- prefer-standby picks the local-AZ replica first
SELECT 1+2;

-- disabling 6434 forces the route to the next standby
RESET __spqr__execute_on;
SELECT __spqr__console_execute('ALTER SHARD sh1 OPTIONS (SET HOST ''127.0.0.1:6434:local PRIORITY -1'')') /*__spqr__preferred_engine: v2 */;

SET __spqr__execute_on TO sh1;
SELECT 1+2;
RESET __spqr__execute_on;

SELECT __spqr__console_execute('ALTER SHARD sh1 OPTIONS (SET HOST ''127.0.0.1:6434:local'')') /*__spqr__preferred_engine: v2 */;
SET __spqr__execute_on TO sh1;

\! iptables -D INPUT -p tcp --dport 6433 -j REJECT
\! iptables -D INPUT -p tcp --dport 6436 -j REJECT
\! iptables -D INPUT -p tcp --dport 6437 -j REJECT

RESET __spqr__target_session_attrs;
RESET __spqr__notice_message_format;
RESET __spqr__execute_on;

SET __spqr__reply_notice TO false;

DROP TABLE pr_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
