CREATE KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;

CREATE SHARDING RULE rule1 COLUMNS id;
CREATE SHARDING RULE rule1 COLUMNS id;
CREATE SHARDING RULE rule2 COLUMNS id;

SHOW key_ranges;
SHOW sharding_rules;

DROP SHARDING RULE rule1;
DROP KEY RANGE krid1;

CREATE KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2;
CREATE SHARDING RULE rule1 COLUMNS id;

SHOW key_ranges;
SHOW sharding_rules;

CREATE SHARDING RULE cat TABLE orders COLUMN iid;
CREATE SHARDING RULE dog TABLE delivery COLUMN order_id;

SHOW sharding_rules;

CREATE KEY RANGE krid2 FROM 33 TO 44 ROUTE TO nonexistentshard;

DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;

