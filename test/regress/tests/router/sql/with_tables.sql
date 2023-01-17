CREATE TABLE orders(iid INT PRIMARY KEY);
CREATE TABLE delivery(iid INT PRIMARY KEY, order_id INT, FOREIGN KEY(order_id) REFERENCES orders(iid));

INSERT INTO orders(iid) VALUES (5);
INSERT INTO delivery(iid,order_id) VALUES (10, 5);
SELECT * FROM delivery;
SELECT * FROM delivery JOIN orders ON order_id = iid;
SELECT * FROM delivery JOIN orders ON delivery.order_id = orders.iid;
