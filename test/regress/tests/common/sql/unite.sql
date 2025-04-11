CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;

UNITE KEY RANGE krid1 WITH krid2;
SHOW key_ranges;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
