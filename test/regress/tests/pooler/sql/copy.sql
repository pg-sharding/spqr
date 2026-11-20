
BEGIN;

CREATE TABLE copy_test(id INTEGER);

SELECT FROM copy_test WHERE id = 29;

SET __spqr__allow_multishard TO true;
COPY copy_test(id) FROM STDIN /* __spqr__allow_multishard: true, __spqr__reply_notice: true */;
10
112
113
114
115
\.

ROLLBACK;

