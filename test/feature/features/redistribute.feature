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
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh3",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh2",
      "Locked":"false"
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
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Locked":"false"
    },
    {
      "Key range ID":"kr2",
      "Distribution ID":"ds1",
      "Lower bound":"100",
      "Shard ID":"sh2",
      "Locked":"false"
    },
    {
      "Key range ID":"kr3",
      "Distribution ID":"ds1",
      "Lower bound":"150",
      "Shard ID":"sh1",
      "Locked":"false"
    },
    {
      "Key range ID":"kr4",
      "Distribution ID":"ds1",
      "Lower bound":"90",
      "Shard ID":"sh2",
      "Locked":"false"
    }]
    """
  
  Scenario: REDISTRIBUTE KEY RANGE works with hashed distribution
    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds2 COLUMN TYPES UUID;
    ALTER DISTRIBUTION ds2 ATTACH RELATION xMove3 DISTRIBUTION KEY w_id;
    """
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr2 FROM '00000000-0000-0000-000000000000' ROUTE TO sh1 FOR DISTRIBUTION ds2;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove3(w_id TEXT, s TEXT);
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
      "Key range ID":"kr2",
      "Distribution ID":"ds2",
      "Lower bound":"'00000000-0000-0000-000000000000'",
      "Shard ID":"sh2",
      "Locked":"false"
    }]
    """
