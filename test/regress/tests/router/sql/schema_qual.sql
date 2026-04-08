
\c spqr-console
CREATE DISTRIBUTION d (integer);
CREATE KEY RANGE k2 from 10 route to sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE k1 from 0 route to sh1 FOR DISTRIBUTION d;
CREATE RELATION r (i);

\c regress

SET __spqr__maintain_params TO true;

CREATE SCHEMA sh1;
CREATE SCHEMA sh2;

CREATE TABLE sh1.r(i int);
CREATE TABLE sh2.r(i int);

INSERT INTO sh1.r(i) VALUES (1);
INSERT INTO sh2.r(i) VALUES (11);

SET search_path TO 'sh1';

SELECT * FROM r WHERE i = 1;

SET search_path TO 'sh2';

SELECT * FROM r WHERE i = 11;

SELECT * FROM sh1.r WHERE i = 1;
SELECT * FROM sh2.r WHERE i = 11;

\c spqr-console

alter distribution d alter relation r schema sh1;
create relation sh2.r(i) in d;

\c regress

SET __spqr__maintain_params TO true;

SET search_path TO 'sh1';

SELECT * FROM r WHERE i = 1;

SET search_path TO 'sh2';

SELECT * FROM r WHERE i = 11;

SELECT * FROM sh1.r WHERE i = 1;
SELECT * FROM sh2.r WHERE i = 11;

DROP SCHEMA sh1 CASCADE;
DROP SCHEMA sh2 CASCADE;

\c spqr-console

DROP DISTRIBUTION ALL CASCADE;
