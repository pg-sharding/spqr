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

-- should exists 
TABLE tt;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
