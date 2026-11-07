\c spqr-console
CREATE DISTRIBUTION test_hint_distribution (integer);
CREATE KEY RANGE item_range FROM 0 ROUTE TO sh1 FOR DISTRIBUTION test_hint_distribution;
CREATE RELATION item (id) FOR DISTRIBUTION test_hint_distribution;

\c regress
CREATE TABLE item(id bigint);
INSERT INTO item VALUES (2972989404);
SELECT * FROM item /* __spqr__sharding_key: 2972989404 */;

\c spqr-console
CREATE DISTRIBUTION dummy (integer);
\c regress
SELECT * FROM item /* __spqr__sharding_key: 2972989404 */;
DROP TABLE item;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
