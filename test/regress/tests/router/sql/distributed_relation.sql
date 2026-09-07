\c spqr-console

CREATE DISTRIBUTION ds1 (integer hash);
CREATE KEY RANGE kr2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE RELATION t_city (id HASH CITY) FOR DISTRIBUTION ds1;
CREATE RELATION t_murmur (id HASH MURMUR) FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE t_city(id int, val int);
CREATE TABLE t_murmur(id int, val int);

INSERT INTO t_city(id, val) VALUES (57, 1) /* __spqr__sharding_key: 57, __spqr__distribution: ds1, __spqr__distributed_relation: t_city */;
INSERT INTO t_murmur(id, val) VALUES (57, 2) /* __spqr__sharding_key: 57, __spqr__distribution: ds1, __spqr__distributed_relation: t_murmur */;

-- ambiguous hash func
SELECT __spqr__route_key('ds1', '57') /* __spqr__distribution: ds1 */;

-- routes to different shards
SELECT __spqr__route_key('ds1', '57') /* __spqr__distribution: ds1, __spqr__distributed_relation: t_city */;

SELECT __spqr__route_key('ds1', '57') /* __spqr__distribution: ds1, __spqr__distributed_relation: t_murmur */;

SELECT * FROM t_city /* __spqr__sharding_key: 57, __spqr__distribution: ds1, __spqr__distributed_relation: t_city */;

SELECT * FROM t_murmur /* __spqr__sharding_key: 57, __spqr__distribution: ds1, __spqr__distributed_relation: t_murmur */;

SHOW __spqr__distributed_relation; -- error

DROP TABLE t_city;
DROP TABLE t_murmur;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
