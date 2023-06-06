DROP TABLE IF EXISTS transactions_test;
CREATE TABLE transactions_test (id int);

BEGIN;
SELECT * FROM transactions_test WHERE id = 1;;
INSERT INTO transactions_test (id) VALUES (1);
SELECT * FROM transactions_test WHERE id = 1;;
ROLLBACK;

SELECT * FROM transactions_test WHERE id = 1;;

BEGIN;
SELECT * FROM transactions_test WHERE id = 1;;
INSERT INTO transactions_test (id) VALUES (1);
SELECT * FROM transactions_test WHERE id = 1;;
COMMIT;

SELECT * FROM transactions_test WHERE id = 1;;