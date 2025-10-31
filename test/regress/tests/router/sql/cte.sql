\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE REFERENCE TABLE ref_rel_1;

ALTER DISTRIBUTION ds1 ATTACH RELATION table1 DISTRIBUTION KEY i;
CREATE DISTRIBUTED RELATION table2 (a) FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE table1(i INT PRIMARY KEY);
CREATE TABLE table2(a INT, b INT, c INT);
CREATE TABLE ref_rel_1(i int, j int);

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) TABLE s;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT 1;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT (select * from s), 2;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT 1 + 2;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
), s2 AS (SELECT * FROM s) TABLE s2;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 210
), s2 AS (SELECT * FROM table1 WHERE i = 288) TABLE s2;


-- XXX: fix parser to allow this
--WITH s AS (
--	SELECT 1 FROM table1 WHERE i = 210
--), s2 AS (SELECT * FROM table1 WHERE i = 288) INSERT INTO table1 (select * from s2 union all select * from s);

INSERT INTO table1 (i) VALUES(10);
WITH vv AS (SELECT 1) INSERT INTO table1 (i) VALUES(11);

WITH vv AS (SELECT i + 1 FROM table1 WHERE i = 11) INSERT INTO table1 (i) TABLE vv;

INSERT INTO table1 WITH s AS (SELECT i + 1 FROM table1 WHERE i = 12) TABLE s;

SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh1 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh2 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh3 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh4 */;

INSERT INTO table2 (a,b,c) VALUES (1, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (2, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (101, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (301, 22, 33);

WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM table2 t, vv WHERE t.a = vv.x;
WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM table2 t, vv v WHERE t.a = v.x;


WITH vals (x, y, z) AS (
    VALUES (
		1,
		2,
		4
    )
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;


WITH vals (x) AS (
    VALUES (
		1
    ), (2),
	(101),
	(301)
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;


WITH vals (y, z, x) AS (
    VALUES (
		2,
		4,
		1
    )
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;

WITH vals (x, y, z) AS (
    VALUES (
		1,
		2,
		4
    )
),
ttttt AS (
    SELECT 
		*
    FROM table2 r
    JOIN vals 
        ON r.a = vals.x
)
UPDATE table2 SET b = b + 1;

SELECT * FROM table2 ORDER BY a /* __spqr__execute_on: sh1 */;




DROP TABLE table1;
DROP TABLE table2;
DROP TABLE ref_rel_1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;





WITH activations_count AS (
  SELECT 
    count(check_id_tab.check_id) AS activations_count, 
    check_id_tab.campaign_id AS campaign_id 
  FROM 
    (
      SELECT 
    	1
      FROM 
        cashback.calculations AS detail_calc 
        JOIN cashback.calculations AS final_calc ON detail_calc.operation_id = final_calc.operation_id 
        AND detail_calc.mechanic = final_calc.mechanic 
        AND final_calc.campaign_id IS NULL 
      WHERE 
        detail_calc.user_id = 'c411f63b-9492-4dc9-8563-69ba2bf92fa5' :: uuid 
      GROUP BY 
        final_calc.check_id, 
        detail_calc.item_id, 
        detail_calc.campaign_id, 
        detail_calc.strategy_id 
      HAVING 
        sum(detail_calc.amount) > 0
    ) AS check_id_tab 
  GROUP BY 
    check_id_tab.campaign_id
) 
SELECT 
  DISTINCT ON (
    cashback.calculations.campaign_id
  ) cashback.calculations.campaign_id AS id, 
  (
    SELECT 
      activations_count.activations_count 
    FROM 
      activations_count 
    WHERE 
      activations_count.campaign_id = cashback.calculations.campaign_id
  ) AS "immediateActivationsCount"
FROM 
  cashback.calculations 
  JOIN cashback.campaigns AS campaigns_1 ON cashback.calculations.campaign_id = campaigns_1.id 
WHERE 
  cashback.calculations.user_id = 'c411f63b-9492-4dc9-8563-69ba2bf92fa5' :: uuid 

