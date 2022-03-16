\set aid random(1, 100000)

\set abalance random(1, 1000)

\set delta random(-5000,5000)

BEGIN;

INSERT INTO x (w_id, abalance) VALUES (:aid, :abalance);

UPDATE x SET abalance = abalance + :delta  WHERE w_id = :aid;

SELECT abalance FROM x WHERE w_id = :aid;

END;
