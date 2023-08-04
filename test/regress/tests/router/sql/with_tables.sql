\c spqr-console

ADD SHARDING RULE t1 TABLE orders COLUMN id;
ADD SHARDING RULE t2 TABLE delivery COLUMN order_id;

ADD KEY RANGE krid1 FROM 1 TO 101 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 101 TO 201 ROUTE TO sh2;

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
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
