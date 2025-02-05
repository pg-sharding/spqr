\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;

ALTER DISTRIBUTION ds1 ATTACH RELATION tt DISTRIBUTION KEY i;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 30 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 70 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid4 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE tt(i INT) /*  */;

BEGIN;

INSERT INTO tt (i) VALUES(1);

DROP TABLE tt;

INSERT INTO tt (i) VALUES(1);
INSERT INTO tt (i) VALUES(1);
INSERT INTO tt (i) VALUES(1);
INSERT INTO tt (i) VALUES(1);

ROLLBACK;

BEGIN;

-- error while query planning.
SELECT j FROM ttr /* __spqr__engine_v2: true */;

-- errors
SELECT;
SELECT;
SELECT;

ROLLBACK;

SET __spqr__default_route_behaviour TO BLOCK;

BEGIN;

-- try scatter out, but error because of DRB.
SELECT j FROM tt;

-- errors
SELECT;
SELECT;
SELECT;

ROLLBACK;

SET __spqr__default_route_behaviour TO ALLOW;

BEGIN;

-- scatter out, recieve error from shards.
SELECT j FROM tt /* __spqr__engine_v2: true */;

-- errors
SELECT;
SELECT;
SELECT;

ROLLBACK;

-- should exists 
TABLE tt;
DROP TABLE tt;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
