\c spqr-console
CREATE REFERENCE RELATION ref_pinned;

\c regress
CREATE TABLE ref_pinned(id INT);

SET __spqr__session_connections_pin TO on;
BEGIN;
\set VERBOSITY SQLSTATE
INSERT INTO ref_pinned(missing) VALUES(1);
COMMIT;
SELECT 42 /* __spqr__execute_on: sh1 */;
RESET __spqr__session_connections_pin;

DROP TABLE ref_pinned;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
