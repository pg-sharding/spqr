\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION xxmixed DISTRIBUTION KEY id;

\c regress

CREATE TABLE xxmixed(id int);
INSERT INTO xxmixed (id) VALUES(1);
INSERT INTO xxmixed (id) VALUES(10);
INSERT INTO xxmixed (id) VALUES(11);
INSERT INTO xxmixed (id) VALUES(20);

INSERT INTO xxmixed (id) VALUES(21);
INSERT INTO xxmixed (id) VALUES(22);
INSERT INTO xxmixed (id) VALUES(29);
INSERT INTO xxmixed (id) VALUES(30);
INSERT INTO xxmixed (id) VALUES(30);
INSERT INTO xxmixed (id) VALUES(30);
INSERT INTO xxmixed (id) VALUES(30);
INSERT INTO xxmixed (id) VALUES(30);

SELECT * FROM xxmixed ORDER BY id;

UPDATE xxmixed SET id = -1 /* __spqr__engine_v2: true */;;
DELETE FROM xxmixed /* __spqr__engine_v2: true */;;

DROP TABLE xxmixed;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
