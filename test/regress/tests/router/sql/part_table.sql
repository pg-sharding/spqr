
-- Check partitioned tables.
\c spqr-console

-- SETUP
CREATE DISTRIBUTION ds1 COLUMN TYPES varchar hash;

CREATE KEY RANGE krid2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

-- the set of all unsigned 32-bit integers (0 to 4294967295)
ALTER DISTRIBUTION ds1 ATTACH RELATION xxhash_part DISTRIBUTION KEY i HASH FUNCTION MURMUR;


\c regress
CREATE TABLE xxhash_part (i uuid, d date) partition by range (d);

create table xxhash_part_1 partition of xxhash_part for values from ('2024-12-01') to ('2024-12-31');
create table xxhash_part_2 partition of xxhash_part for values from ('2024-11-01') to ('2024-11-30');
create table xxhash_part_3 partition of xxhash_part for values from ('2024-10-01') to ('2024-10-31');

insert into xxhash_part (i,d ) values ('b37da10c-25a0-4473-96fd-c3871bd81d00', '2024-12-23');
insert into xxhash_part (i,d ) values ('b1569ed9-4029-495f-8d7c-02b216528c0a', '2024-12-21');
insert into xxhash_part (i,d ) values ('0583484b-e407-41e3-9c75-66c1994944c8', '2024-12-20');
insert into xxhash_part (i,d ) values ('b14f8871-ba67-4244-8fa6-45edabaf6206', '2024-12-22');

insert into xxhash_part (i,d ) values ('7906fd75-60ae-4e1d-99c5-da19d4b63515', '2024-11-23');
insert into xxhash_part (i,d ) values ('99d75dea-c62c-4bb3-9d6e-eda36432bda0', '2024-11-21');
insert into xxhash_part (i,d ) values ('0bd49f01-b01f-4c18-bd44-d75746a7c71c', '2024-11-20');
insert into xxhash_part (i,d ) values ('3dcd025f-b016-4d1c-aaa8-4f8462f9aa8e', '2024-11-22');

insert into xxhash_part (i,d ) values ('93043bc0-b6d3-4bb6-b1b4-869d2b4a4967', '2024-10-23');
insert into xxhash_part (i,d ) values ('f6a01c78-9ce8-4cc3-a78f-32bebc4ccb4a', '2024-10-21');
insert into xxhash_part (i,d ) values ('b3e31fad-80fa-48df-bcc0-51ffd1594df3', '2024-10-20');
insert into xxhash_part (i,d ) values ('d15707df-f256-49a5-a693-3feda5afe678', '2024-10-22');


SELECT * FROM xxhash_part ORDER BY d /* __spqr__execute_on: sh1 */;
SELECT * FROM xxhash_part ORDER BY d /* __spqr__execute_on: sh2 */;

--TEARDOWN
DROP TABLE xxhash_part;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
