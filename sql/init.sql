ADD SHARDING RULE r1 COLUMNS w_id;
ADD SHARDING RULE r2 COLUMNS id;
ADD SHARDING RULE r3 COLUMNS bid;
ADD KEY RANGE krid3 FROM 21 TO 30 ROUTE TO sh2;
ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;
ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
