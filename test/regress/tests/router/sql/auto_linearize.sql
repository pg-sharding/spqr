
\c spqr-console
CREATE DISTRIBUTION d (integer);
CREATE KEY RANGE kridi4 from 3000 route to sh4 FOR DISTRIBUTION d;
CREATE KEY RANGE kridi3 from 2000 route to sh3 FOR DISTRIBUTION d;
CREATE KEY RANGE kridi2 from 1000 route to sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION d;
CREATE RELATION rel_al (id);

CREATE REFERENCE RELATION rf_al;

\c regress


CREATE TABLE rel_al(id INT UNIQUE, c INT);
CREATE TABLE rf_al(id INT UNIQUE, c INT);


INSERT INTO rel_al (id, c) VALUES (1111, 1) ON CONFLICT (id) DO UPDATE SET c = rel_al.c + 1;

INSERT INTO rel_al (id, c) VALUES (1111, 77) ON CONFLICT (id) DO UPDATE SET c = rel_al.c + 1;

INSERT INTO rel_al (id, c) VALUES (1111, 881), (2222, 1) ON CONFLICT (id) DO UPDATE SET c = rel_al.c + 1;

SELECT __spqr__ctid('rel_al');

WITH v (id, c) AS (VALUES (2222, 888), (3333, 1)) 
INSERT INTO rel_al (id, c) SELECT id, c FROM v ON CONFLICT (id) DO UPDATE SET c = rel_al.c + 1;

WITH v (id, c) AS (VALUES (3330, 888), (3333, 1)) 
INSERT INTO rel_al (id, c) SELECT id, c FROM v ON CONFLICT (id) DO UPDATE SET c = rel_al.c + 1;

SELECT __spqr__ctid('rel_al');


INSERT INTO rf_al (id, c) VALUES (1111, 1) ON CONFLICT (id) DO UPDATE SET c = rf_al.c + 1;

INSERT INTO rf_al (id, c) VALUES (1111, 777), (2222, 1) ON CONFLICT (id) DO UPDATE SET c = rf_al.c + 1;

SELECT __spqr__ctid('rf_al');

WITH v (id, c) AS (VALUES (1221, 888), (1222, 1)) 
INSERT INTO rf_al (id, c) SELECT id, c FROM v ON CONFLICT (id) DO UPDATE SET c = rf_al.c + 1;

SELECT __spqr__ctid('rf_al');

DROP TABLE rel_al;
DROP TABLE rf_al;


\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
