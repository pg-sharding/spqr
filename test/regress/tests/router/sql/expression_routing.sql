
\c spqr-console
create distribution ds1 column types int hash;
create relation tr(MURMUR [id1 INT HASH, id2 VARCHAR HASH]);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1;

show relations;


\c regress
CREATE TABLE tr (id1 INT, val TEXT);
CREATE TABLE tr (id1 INT, id2 TEXT, val TEXT);

COPY tr (id1, id2, val) FROM STDIN DELIMITER '|';
0|aba|ababababb
1|ziziziz|abab
2|ziiziziziz|ababa
3|ziizizizi|abababab
4|yyuyuyuyu|ababba
\.

SELECT * FROM tr WHERE id1 = 0 AND id2 = 'aba';
SELECT * FROM tr WHERE id1 = 1 AND id2 = 'ziziziz';
SELECT * FROM tr WHERE id1 = 2 AND id2 = 'ziiziziziz';
SELECT * FROM tr WHERE id1 = 3 AND id2 = 'ziizizizi';
SELECT * FROM tr WHERE id1 = 4 AND id2 = 'yyuyuyuyu';

INSERT INTO tr (id1, id2, val) VALUES (0, 'aba', 'asas');
INSERT INTO tr (id1, id2, val) VALUES (1, 'ziziziz', 'assaas');
INSERT INTO tr (id1, id2, val) VALUES (2, 'ziiziziziz', 'asssa');
INSERT INTO tr (id1, id2, val) VALUES (3, 'ziizizizi', 'asas');
INSERT INTO tr (id1, id2, val) VALUES (4, 'yyuyuyuyu', 'saassa');

SELECT * FROM tr WHERE id1 = 0 AND id2 = 'aba';
SELECT * FROM tr WHERE id1 = 1 AND id2 = 'ziziziz';
SELECT * FROM tr WHERE id1 = 2 AND id2 = 'ziiziziziz';
SELECT * FROM tr WHERE id1 = 3 AND id2 = 'ziizizizi';
SELECT * FROM tr WHERE id1 = 4 AND id2 = 'yyuyuyuyu';

WITH vals (x, y, z) AS (VALUES(1, 'ziziziz', 32), (2, 'ziiziziziz', 32)) 
    SELECT * FROM 
        tr JOIN vals ON tr.id1 = vals.x AND tr.id2 = vals.y;


-- TODO: fix
WITH vals (x, y, z) AS (VALUES (4, 'yyuyuyuyu', 32), (1, 'ziziziz', 32)) 
    SELECT * FROM 
        tr JOIN vals ON tr.id1 = vals.x AND tr.id2 = vals.y /* __spqr__engine_v2: true */;


WITH vals (x, y, z) AS (VALUES (4, 'yyuyuyuyu', 32), (1, 'ziziziz', 32), (0, 'aba', 3), (1, 'ziziziz', 3)) 
    SELECT * FROM 
        tr JOIN vals ON tr.id1 = vals.x AND tr.id2 = vals.y /* __spqr__engine_v2: true */;


UPDATE tr SET val = 1123 WHERE id1 = 1 AND id2 = 'ziziziz';

DELETE FROM tr WHERE id1 = 1 AND id2 = 'ziziziz' RETURNING *;

DROP TABLE tr;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;