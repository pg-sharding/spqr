

CREATE TABLE xMove(w_id INT, s TEXT);
INSERT INTO xMove(w_id, s) VALUES (1, '001');
INSERT INTO xMove(w_id, s) VALUES (11, '002');
SELECT * FROM xMove WHERE w_id = 11;
LOCK KEY RANGE krid2;;
MOVE KEY RANGE krid2 to sh1;;
UNLOCK KEY RANGE krid2;;
SELECT * FROM xMove WHERE w_id = 11;