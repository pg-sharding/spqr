\c spqr-console

-- check that numeric type works
CREATE SHARDING RULE t1 COLUMNS id;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2;

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

UPDATE xxmixed SET id = -1;
DELETE FROM xxmixed;


DROP TABLE xxmixed;

\c spqr-console
DROP DATASPACE ALL CASCADE;
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;