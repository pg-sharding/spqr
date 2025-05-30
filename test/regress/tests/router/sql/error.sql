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

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
