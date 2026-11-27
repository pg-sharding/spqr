\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 1 route to sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION x DISTRIBUTION KEY id;

\c regress

CREATE TABLE x(id int);

SELECT * FROM x WHERE id = 1;
SELECT * FROM x WHERE ixxxd = 1 /* __spqr__engine_v2: true */;
SELECT * FROM x WHERE ixxxd = 1 iuwehiuhweui;
SELECT * FROM x WHERE id = 1;

DROP TABLE x;


/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

-- A linearized query error must not leave unread responses on pinned connections.
SET __spqr__session_connections_pin TO on;
SELECT 1/0 /* __spqr__scatter_query: true, __spqr__linearize_dispatch: true */;
SELECT 42 /* __spqr__execute_on: sh1 */;
SELECT 43 /* __spqr__execute_on: sh1 */;
RESET __spqr__session_connections_pin;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
