\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

\c regress

SET __spqr__reply_notice TO false;

SET application_name TO 'regress';
SHOW application_name;

BEGIN;
SET LOCAL application_name TO 'regress_local';
SHOW application_name;
ROLLBACK;
SHOW application_name;

BEGIN;
SET LOCAL application_name TO 'regress_local';
SHOW application_name;
COMMIT;
SHOW application_name;

BEGIN;
SET application_name TO 'regress_tx';
SHOW application_name;
ROLLBACK;
SHOW application_name;

BEGIN;
SET application_name TO 'regress_tx';
SHOW application_name;
COMMIT;
SHOW application_name;

BEGIN;
RESET application_name;
SHOW application_name;
ROLLBACK;

BEGIN;
SET x TO '1';
SHOW x;
SHOW application_name;
RESET ALL;
SHOW application_name;
SHOW x;
ROLLBACK;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
