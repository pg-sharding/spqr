
\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION tlt1 DISTRIBUTION KEY i;

\c regress

CREATE SCHEMA sh2;
CREATE TABLE sh2.tlt1(i int, j int);

INSERT INTO sh2.tlt1 (i, j) VALUES(1, 12);
INSERT INTO sh2.tlt1 (i, j) VALUES(1, 12);
INSERT INTO sh2.tlt1 (i, j) VALUES(2, 13);
INSERT INTO sh2.tlt1 (i, j) VALUES(2, 13);
INSERT INTO sh2.tlt1 (i, j) VALUES(12, 12);
INSERT INTO sh2.tlt1 (i, j) VALUES(12, 14);
INSERT INTO sh2.tlt1 (i, j) VALUES(122, 124);
INSERT INTO sh2.tlt1 (i, j) VALUES(112, 124);
INSERT INTO sh2.tlt1 (i, j) VALUES(113, 125);

select (select sum(j) from sh2.tlt1 where i = 112);
select (select sum(j) as xyx from sh2.tlt1 where i = 112) as aboba;
select (select sum(j) from sh2.tlt1 where i = 112), (select sum(j) from sh2.tlt1 where sh2.tlt1.i = 113);
select  coalesce((select sum(j) from sh2.tlt1 where i = 1), 0), coalesce((select sum(j) from sh2.tlt1 where i = 2 and j not in (select 12)), 0);


/* test quoted sconsts. XXX: find a better place for that */

SELECT 'a=b, "a=b", '' c '' '' '' ', 'a=b, "a=b", '' c ''''''' /* __spqr__.execute_on: sh3 */;

DROP SCHEMA sh2 CASCADE;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;

