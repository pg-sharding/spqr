Feature: Redistribution test
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
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    REGISTER ROUTER r2 ADDRESS "[regress_router_2]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with multiple relations
    When I execute SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
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
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """


  Scenario: REDISTRIBUTE KEY RANGE works with newly added shard
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
  
  Scenario: Key range lock during redistribution works
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
    When I run command on host "router"
    """
    cat > ./check_router.sh <<EOF
    #!/bin/bash
    set -e

    while true; do
      res=$(psql -t -h localhost -U regress -c "SELECT * FROM xMove WHERE w_id = 1")
      if ! $? && [ "x$res" == "x0" ]; then
        echo Incorrect result
      else
        echo All good
      fi
    done
    EOF
    chmod +x check_router.sh
    ./check_router.sh > ./check_router.log &
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run command on host "router"
    """
    grep -v 'All good' check_router.log
    """
    Then command return code should be "1"

  Scenario: REDISTRIBUTE KEY RANGE APPLY works
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
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 100 APPLY;
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  
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
    ALTER DISTRIBUTION ds1 ALTER RELATION xMove SCHEMA my_schema;
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with multiple schemas
    When I execute SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ALTER RELATION xMove SCHEMA my_schema;
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with many keys on single distribution key
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 9), 'sample text value' FROM generate_series(1, 150);
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
    1500
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  

  Scenario: REDISTRIBUTE KEY RANGE orders key ranges correctly
    When I execute SQL on host "coordinator"
    """ 
    CREATE KEY RANGE kr3 FROM 150 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr4 FROM 90 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 99), 'sample text value';
    INSERT INTO xMove (w_id, s) SELECT generate_series(150, 199), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(100, 149), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "200" seconds
    """
    REDISTRIBUTE KEY RANGE kr4 TO sh2 BATCH SIZE 10;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    \{"count":140\}
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    \{"count":60\}
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"100",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"150",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"90",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"100",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"150",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"90",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"100",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"150",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"90",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  
  Scenario: REDISTRIBUTE KEY RANGE works with UUID column type
    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds2 COLUMN TYPES UUID;
    ALTER DISTRIBUTION ds2 ATTACH RELATION xMove3 DISTRIBUTION KEY w_id;
    """
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr2 FROM '00000000-0000-0000-0000-000000000000' ROUTE TO sh1 FOR DISTRIBUTION ds2;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove3(w_id uuid, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove3 (w_id, s) SELECT gen_random_uuid(), 'sample text value' FROM generate_series(0, 999);
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
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"'00000000-0000-0000-0000-000000000000'",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"'00000000-0000-0000-0000-000000000000'",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr2",
      "distribution_id":"ds2",
      "lower_bound":"'00000000-0000-0000-0000-000000000000'",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works with mismatched schema on shards
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(s TEXT, w_id INT);
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
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  
  Scenario: REDISTRIBUTE KEY RANGE works when generating several batches of bounds
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 299), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 10;
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
    300
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: REDISTRIBUTE KEY RANGE works in parallel when transferring between different shards
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr4 FROM 900 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr3 FROM 600 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 300 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 599), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(600, 1199), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" in parallel with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 10;
    REDISTRIBUTE KEY RANGE kr2 TO sh3 BATCH SIZE 10;
    REDISTRIBUTE KEY RANGE kr3 TO sh1 BATCH SIZE 10;
    REDISTRIBUTE KEY RANGE kr4 TO sh3 BATCH SIZE 10;
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    300
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    300
    """
    When I run SQL on host "shard3"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    600
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh3",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"600",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"900",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh3",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"600",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"900",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh3",
      "locked":"false"
    },
    {
      "key_range_id":"kr3",
      "distribution_id":"ds1",
      "lower_bound":"600",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr4",
      "distribution_id":"ds1",
      "lower_bound":"900",
      "shard_id":"sh3",
      "locked":"false"
    }]
    """
  
  Scenario: REDISTRIBUTE KEY RANGE works in parallel when transferring between same shards
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr2 FROM 300 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 599), 'sample text value';
    """
    Then command return code should be "0"
    # Create FDW (does not work very well in parallel)
    When I run SQL on host "coordinator"
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 10 CHECK;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" in parallel with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 10;
    REDISTRIBUTE KEY RANGE kr2 TO sh2 BATCH SIZE 10;
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
    600
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    },
    {
      "key_range_id":"kr2",
      "distribution_id":"ds1",
      "lower_bound":"300",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  
  Scenario: Cannot redistribute same key range twice
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
            "id":            "tgid1",
            "shard_to_id":   "sh2",
            "kr_id_from":    "kr1",
            "kr_id_to":      "krid2",
            "type":          1,
            "limit":         -1,
            "coeff":         0.75,
            "bound_rel":     "test",
            "total_keys":    200,
            "task":
            {
                "id":            "2",
                "kr_id_temp":    "temp_id",
                "bound":         ["FAAAAAAAAAA="],
                "state":         0,
                "task_group_id": "tgid1"
            }
        }
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    REDISTRIBUTE KEY RANGE "kr1" TO "sh2";
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    there is already a move task group .*tgid1.* for key range .*kr1.*
    """


  Scenario: move task group status is error after coordinator crash
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
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 4999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator", then stop the host after "10" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 1;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator2"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
        "destination_shard_id":     "sh2",
        "source_key_range_id":      "kr1",
        "batch_size":               "1",
        "state":                    "RUNNING",
        "error":                    ""
    }]
    """
    When we wait for "40" seconds
    When I run SQL on host "coordinator2"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
        "destination_shard_id":     "sh2",
        "source_key_range_id":      "kr1",
        "batch_size":               "1",
        "state":                    "ERROR",
        "error":                    "task group lost running"
    }]
    """
