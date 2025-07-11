Feature: Redistribution with schema test
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_three_shards.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_three_shards.yaml
    """

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
    ALTER DISTRIBUTION ds1 ATTACH RELATION my_schema.xMove DISTRIBUTION KEY w_id;
    ADD SHARD sh1 WITH HOSTS 'postgresql://regress@spqr_shard_1:6432/regress';
    ADD SHARD sh2 WITH HOSTS 'postgresql://regress@spqr_shard_2:6432/regress';
    """
    Then command return code should be "0"

  
  Scenario: REDISTRIBUTE KEY RANGE works with relations inside a schema
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE SCHEMA my_schema;
    CREATE TABLE my_schema.xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO my_schema.xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    ALTER DISTRIBUTION ds1 ALTER RELATION xMove DISTRIBUTION KEY w_id SCHEMA my_schema;
    ALTER DISTRIBUTION ds1 DETACH RELATION xMove2;
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100 APPLY;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM my_schema.xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM my_schema.xMove
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

  Scenario: REDISTRIBUTE KEY RANGE works with multiple schemas
    When I execute SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ALTER RELATION xMove DISTRIBUTION KEY w_id SCHEMA my_schema;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE SCHEMA my_schema;
    CREATE TABLE my_schema.xMove(w_id INT, s TEXT);
    CREATE TABLE xMove2(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO my_schema.xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    INSERT INTO xMove2 (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "300" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100 APPLY;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM my_schema.xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM my_schema.xMove
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
