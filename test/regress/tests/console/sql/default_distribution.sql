CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;

SHOW key_ranges;

CREATE DISTRIBUTION ds2 COLUMN TYPES integer;
CREATE KEY RANGE krid3 FROM 11 ROUTE TO sh2;

SHOW key_ranges;


DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;