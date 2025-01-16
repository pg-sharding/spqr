Feature: Redistribution test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    ADD SHARD sh1 WITH HOSTS 'postgresql://regress@spqr_shard_1:6432/regress';
    ADD SHARD sh2 WITH HOSTS 'postgresql://regress@spqr_shard_2:6432/regress';
    """
    Then command return code should be "0"

  Scenario: REDISTRIBUTE KEY RANGE works with multiple moves
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with single move
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 10000;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with empty relation
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with multiple relations
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    CREATE TABLE xMove2(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    INSERT INTO xMove2 (w_id, s) SELECT generate_series(0, 99), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "200" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    100
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """
  

  Scenario: REDISTRIBUTE KEY RANGE works with hashed distribution
    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds2 COLUMN TYPES VARCHAR HASH;
    ALTER DISTRIBUTION ds2 ATTACH RELATION xMove3 DISTRIBUTION KEY w_id HASH FUNCTION MURMUR;
    """
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr2 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds2;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove3(w_id TEXT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove3 (w_id, s) SELECT cast (generate_series(0, 999) as text), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr2 TO sh2 BATCH SIZE 100;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr2",
      "Distribution ID":"ds2",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """
  

  Scenario: REDISTRIBUTE KEY RANGE works with hashed distribution
    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds2 COLUMN TYPES VARCHAR HASH;
    ALTER DISTRIBUTION ds2 ATTACH RELATION xMove3 DISTRIBUTION KEY w_id HASH FUNCTION MURMUR;
    """
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr2 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds2;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove3(w_id TEXT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove3 (w_id, s) SELECT cast (generate_series(0, 999) as text), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr2 TO sh2;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr2",
      "Distribution ID":"ds2",
      "Lower bound":"0",
      "Shard ID":"sh2"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with newly added shard
    When I execute SQL on host "coordinator"
    """
    ADD SHARD sh3 WITH HOSTS 'postgresql://regress@spqr_shard_3:6432/regress';
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "1500000" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh3 BATCH SIZE 10000;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard3"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh3"
    }]
    """
