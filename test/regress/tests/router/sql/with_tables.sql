\c spqr-console

CREATE SHARDING RULE t1 TABLE orders COLUMN id;
CREATE SHARDING RULE t2 TABLE delivery COLUMN order_id;

CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2;

\c regress

-- check that sharding rule with tables works
CREATE TABLE orders(id INT PRIMARY KEY);

CREATE TABLE delivery(id INT PRIMARY KEY, order_id INT, FOREIGN KEY(order_id) REFERENCES orders(id));

INSERT INTO orders(id) VALUES (5);
INSERT INTO delivery(id,order_id) VALUES (10, 5);
SELECT * FROM delivery;
SELECT * FROM delivery JOIN orders ON order_id = id;
SELECT * FROM delivery JOIN orders ON delivery.order_id = orders.id;

DROP TABLE orders CASCADE;
DROP TABLE delivery;

\c spqr-console
DROP DATASPACE ALL CASCADE;
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;