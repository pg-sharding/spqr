\c spqr-console

CREATE DISTRIBUTION d (integer);
CREATE KEY RANGE k1 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE k0 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION d;
CREATE RELATION r(i);

\c regress

SET __spqr__reply_notice TO false;

CREATE TABLE r(i int, j int, c int);

\d+ r
\dt+ r
\dp+ r

CREATE INDEX ON r USING btree(i);

\d+ r
\dt+ r
\dp+ r

DROP TABLE r;


\c spqr-console

DROP DISTRIBUTION ALL CASCADE;