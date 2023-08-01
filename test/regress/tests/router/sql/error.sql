\c spqr-console
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
CREATE SHARDING RULE r1 COLUMN id;

CREATE KEY RANGE kridi1 from 1 to 11 route to sh1;
CREATE KEY RANGE kridi2 from 11 to 31 route to sh2;

\c regress

CREATE TABLE x(id int);

SELECT * FROM x WHERE id = 1;
SELECT * FROM x WHERE ixxxd = 1;
SELECT * FROM x WHERE ixxxd = 1 iuwehiuhweui;
SELECT * FROM x WHERE id = 1;

DROP TABLE x;
\c spqr-console
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
