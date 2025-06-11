\c spqr-console
CREATE REFERENCE TABLE rf_seq_xtab AUTO INCREMENT id1 START 42, id2 START 43;

\c regress

CREATE TABLE rf_seq_xtab (id1 INT, id2 INT, value INT);

INSERT INTO rf_seq_xtab (value) VALUES(3) /* __spqr__engine_v2: true */;

TABLE rf_seq_xtab;


DROP TABLE rf_seq_xtab;

\c spqr-console

DROP REFERENCE RELATION rf_seq_xtab;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
