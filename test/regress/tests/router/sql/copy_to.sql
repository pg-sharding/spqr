CREATE TABLE copy_to_test (id int) /* __spqr__scatter_query: true */;


INSERT INTO copy_to_test(id) VALUES (1), (2) /* __spqr__execute_on: sh1 */;

INSERT INTO copy_to_test(id) VALUES (3), (4), (5) /* __spqr__execute_on: sh2 */;

/* nothing on shard3 */;

INSERT INTO copy_to_test(id) VALUES (7), (8),(9),(12)/* __spqr__execute_on: sh4 */;

COPY copy_to_test TO STDOUT;

DROP TABLE copy_to_test;
