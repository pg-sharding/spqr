DROP TABLE IF EXISTS x;
CREATE TABLE x(w_id INT);

INSERT INTO x (w_id) VALUES(1);
INSERT INTO x (w_id) VALUES(11)

SELECT * FROM x WHERE w_id = 11
SELECT * FROM x WHERE w_id = 1

BEGIN;
INSERT INTO x (w_id) VALUES(5);
ROLLBACK;
SELECT * FROM x WHERE w_id = 5;

BEGIN;
INSERT INTO x (w_id) VALUES(5);
COMMIT;
SELECT * FROM x WHERE w_id = 5;