CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

SHOW key_ranges;

DROP KEY RANGE ALL;

SHOW key_ranges;

DROP DISTRIBUTION ALL CASCADE;
