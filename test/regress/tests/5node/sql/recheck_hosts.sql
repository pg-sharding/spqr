-- a single shard sh1 with 1 primary (127.0.0.1:6433) and 4 replicas
-- (127.0.0.1:6434..6437)
\c spqr-console
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
CREATE RELATION tsa_test (i);
\c regress
CREATE TABLE tsa_test (i int);
INSERT INTO tsa_test (i) VALUES (22);
SELECT count(*) FROM tsa_test;
-- kill one replica and mark it dead in the tsa cache
\! iptables -A INPUT -p tcp --dport 6434 -j REJECT
\! sleep 1
SET __spqr__execute_on TO sh1;
SET __spqr__target_session_attrs TO 'prefer-standby';
SET __spqr__execute_host_filter TO ':6434';
SELECT count(*) FROM tsa_test;
RESET __spqr__execute_host_filter;
RESET __spqr__target_session_attrs;
RESET __spqr__execute_on;
-- dead replica must be reported dead
SET __spqr__allow_postprocessing TO true;
SELECT tsa, host, az, alive, match FROM __spqr__run_recheck_hosts() WHERE host = '127.0.0.1:6434' ORDER BY tsa, host;
-- bring the replica back and recheck: it must be alive again
\! iptables -D INPUT -p tcp --dport 6434 -j REJECT
SELECT tsa, host, az, alive, match FROM __spqr__run_recheck_hosts() WHERE host = '127.0.0.1:6434' ORDER BY tsa, host;
SET __spqr__allow_postprocessing TO false;
DROP TABLE tsa_test;
DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
