
\c spqr-console
create distribution ds1 column types int hash;
create relation tr(MURMUR [id1 INT , id2 VARCHAR]);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1;

show relations;


\c regress
CREATE TABLE tr (id1 INT, val TEXT);
CREATE TABLE tr (id1 INT, id2 TEXT, val TEXT);

--COPY tr (id1, id2, val) FROM STDIN DELIMITER '|';
--1|abab|ziziziz
--2|ababa|ziiziziziz
--3|abababab|ziizizizi
--\.

SELECT * FROM tr WHERE id1 = 1 AND id2 = 'ziziziz';
SELECT * FROM tr WHERE id1 = 2 AND id2 = 'ziiziziziz';
SELECT * FROM tr WHERE id1 = 3 AND id2 = 'ziizizizi';

DROP TABLE tr;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;