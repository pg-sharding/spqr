Feature: Coordinator test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    ROUTER_CONFIG_2=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer; 
    CREATE KEY RANGE krid2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 50 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test(id int, name text)
    """
    Then command return code should be "0"


  Scenario: Add/Remove distribution works
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1_test COLUMN TYPES integer;
    CREATE KEY RANGE krid22 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1_test;
    CREATE KEY RANGE krid11 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1_test;
    ALTER DISTRIBUTION ds1_test ATTACH RELATION test1 DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    [
        {
            "Distribution ID":"ds1_test",
            "Column types":"integer"
        }
    ]
    """

    When I run SQL on host "coordinator"
    """
    DROP DISTRIBUTION ds1_test CASCADE
    """
    Then command return code should be "0"
    And qdb should not contain relation "test1"

    When I run SQL on host "coordinator"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    []
    """

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1_test COLUMN TYPES integer;
    CREATE KEY RANGE krid22 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1_test;
    CREATE KEY RANGE krid11 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1_test;
    ALTER DISTRIBUTION ds1_test ATTACH RELATION test1 DISTRIBUTION KEY id;
    """
    Then command return code should be "0"


    When I run SQL on host "coordinator"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    [
        {
            "Distribution ID":"ds1_test",
            "Column types":"integer"
        }
    ]
    """


  Scenario: Add/Remove router works
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then SQL result should match json
    """
    []
    """

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r2 ADDRESS regress_router:7000;
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router -\\u003e r2-regress_router:7000
    """

  Scenario: Register 2 routers with same address fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r2 ADDRESS regress_router:7000
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    router with address regress_router:7000 already exists
    """
    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then SQL result should match regexp
    """
    router -\\u003e r1-regress_router:7000
    """

  Scenario: Register 2 routers with same id fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    router id r1 already exists
    """
    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then SQL result should match regexp
    """
    router -\\u003e r1-regress_router:7000
    """

  Scenario: Register router with invalid address fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r3 ADDRESS invalid_router:7000
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to ping router
    """

    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then SQL result should match json_exactly
    """
    [{
      "show routers":"router -\u003e r1-regress_router:7000",
      "status":"OPENED"
    }]
    """

  Scenario: Unregister router with invalid id does nothing
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r2
    """
    Then command return code should be "0"

  Scenario: Router synchronization after registration works
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1;
    REGISTER ROUTER r1 ADDRESS regress_router:7000
    """
    Then command return code should be "0"
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then SQL result should match json_exactly
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"50",
      "Shard ID":"sh1",
      "Locked":"false"
    },
    {
      "Key range ID":"krid2",
      "Distribution ID":"ds1",
      "Lower bound":"100",
      "Shard ID":"sh2",
      "Locked":"false"
    }]
    """

  Scenario: Add key range with the same id fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid1 FROM 30 ROUTE TO sh1 FOR DISTRIBUTION ds1
    """
    Then SQL error on host "coordinator" should match regexp
    """
    key range krid1 already present in qdb
    """

  Scenario: Lock/Unlock key range works
    Given I run SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1
    """
    When I run SQL on host "router"
    """
    SELECT name FROM test WHERE id=70
    """
    Then SQL error on host "router" should match regexp
    """
    key range .* is locked
    """

    Given I run SQL on host "coordinator"
    """
    UNLOCK KEY RANGE krid1
    """
    When I run SQL on host "router"
    """
    INSERT INTO test(id, name) VALUES(70, 'random_word');
    SELECT name FROM test WHERE id=70
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    random_word
    """

  Scenario: Split/Unite key range works
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid1 BY 70;
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"50",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"70",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"50",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"70",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

    When I run SQL on host "coordinator"
    """
    UNITE KEY RANGE krid1 WITH krid3;
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"50",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"50",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

  Scenario: Split/Unite locked key range fails
    When I run SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    SPLIT KEY RANGE krid3 FROM krid1 BY 5;
    """
    Then SQL error on host "coordinator" should match regexp
    """
    context deadline exceeded
    """

    When I run SQL on host "coordinator"
    """
    UNITE KEY RANGE krid1 WITH krid2
    """
    Then SQL error on host "coordinator" should match regexp
    """
    context deadline exceeded
    """

    When I run SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1NoExistS;
    """
    Then SQL error on host "coordinator" should match regexp
    """
    cant't lock non existent key range
    """


  Scenario: Unite non-adjacent key ranges fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 30 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid3 WITH krid2
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to unite non-adjacent key ranges
    """

  Scenario: Unite in reverse order works
    When I run SQL on host "coordinator"
    """
    DROP KEY RANGE krid3;
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid3 WITH krid1
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW key_ranges
    """
    Then SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"31",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

  Scenario: Unite key ranges routing different shards fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid1 WITH krid3
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to unite key ranges routing different shards
    """

  Scenario: Split key range by bound out of range fails
    #
    # Check we cannot split by bound greater than next key range bound
    #
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid1 BY 120
    """
    Then SQL error on host "coordinator" should match regexp
    """
    bound intersects with.*krid2.*key range
    """

    #
    # Check we cannot split by bound less than lower bound
    #
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid2 BY 10
    """
    Then SQL error on host "coordinator" should match regexp
    """
    bound is out of key range
    """

    #
    # Check we cannot split by lower
    #
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid2 BY 100
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to split because bound equals lower of the key range
    """

  Scenario: Adding/dropping shards works
    When I run SQL on host "coordinator"
    """
    ADD SHARD sh3 WITH HOSTS spqr_shard_1:6432;
    ADD SHARD sh4 WITH HOSTS spqr_shard_2:6432;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW shards;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "shard":"sh1"
      },
      {
        "shard":"sh2"
      },
      {
        "shard":"sh3"
      },
      {
        "shard":"sh4"
      }
    ]
    """

    When I run SQL on host "coordinator"
    """
    ADD SHARD sh3 WITH HOSTS yandex:6432;
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    shard with id sh3 already exist
    """
    When I run SQL on host "coordinator"
    """
    SHOW hosts;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "shard":"sh1",
        "host":"spqr_shard_1:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      },
      {
        "shard":"sh1",
        "host":"spqr_shard_1_replica:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      },
      {
        "shard":"sh2",
        "host":"spqr_shard_2:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      },
      {
        "shard":"sh2",
        "host":"spqr_shard_2_replica:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      },
      {
        "shard":"sh3",
        "host":"spqr_shard_1:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      },
      {
        "shard":"sh4",
        "host":"spqr_shard_2:6432",
        "alive":"unknown",
        "rw":"unknown",
        "time":"unknown"
      }
    ]
    """

    When I run SQL on host "coordinator"
    """
    DROP SHARD sh1 CASCADE;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW shards;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "shard":"sh2"
      },
      {
        "shard":"sh3"
      },
      {
        "shard":"sh4"
      }
    ]
    """

  Scenario: Router is down
    #
    # Coordinator doesn't unregister router
    #
    Given host "router" is stopped
    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router -\\u003e r1-regress_router:7000
    """

    #
    # Coordinator doesn't crash on action
    #
    Given I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh1
    """

    Given host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    []
    """

  Scenario: QDB is down
    Given host "qdb01" is stopped
    And we wait for "5" seconds
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE to sh1 FOR DISTRIBUTION ds1
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    console is in read only mode
    """

    When I run SQL on host "coordinator"
    """
    SHOW ROUTERS
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    context deadline exceeded
    """

  Scenario: QDB is restarted
    Given host "coordinator2" is stopped
    And host "qdb01" is stopped
    And we wait for "5" seconds
    And host "qdb01" is started
    And we wait for "5" seconds
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE to sh1 FOR DISTRIBUTION ds1
    """
    Then command return code should be "0"

  Scenario: Coordinator can restart
    #
    # Coordinator is Up
    #
    Given host "coordinator2" is stopped
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh1 FOR DISTRIBUTION ds1
    """
    Then command return code should be "0"

    #
    # Coordinator has been restarted
    #
    Given host "coordinator" is stopped
    And host "coordinator" is started
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"31",
      "Shard ID":"sh1",
      "Locked":"false"
    }]
    """

  Scenario: Registering router after sharding setup works
    When I run SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 DETACH RELATION test;
    DROP DISTRIBUTION ds1 CASCADE;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000
    """
    Then command return code should be "0"

    When I run SQL on host "router-admin"
    """
    SHOW distributions;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Distribution ID": "ds1",
        "Column types": "integer",
        "Default shard": "not exists"
      }
    ]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Locked":"false"
    },
    {
      "Key range ID":"krid2",
      "Distribution ID":"ds1",
      "Lower bound":"11",
      "Shard ID":"sh2",
      "Locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW relations;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Relation name": "test",
        "Distribution ID": "ds1",
        "Distribution key": "(\"id\", identity)",
        "Schema name": "$search_path"
      }
    ]
    """

  Scenario: Dropping move task group works
    When I record in qdb move task group
    """
    {
            "shard_to_id":   "sh_to",
            "kr_id_from":    "krid1",
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
                "state":         0
            }
        }
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "Destination shard ID":     "sh_to",
        "Source key range ID":      "krid1",
        "Destination key range ID": "krid2"
    }]
    """
    When I run SQL on host "coordinator"
    """
    SHOW move_task
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "Move task ID":             "2",
        "State":                    "PLANNED",
        "Bound":                    "10",
        "Temporary key range ID":   "temp_id"
    }]
    """
    When I run SQL on host "coordinator"
    """
    DROP TASK GROUP
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """
    When I run SQL on host "coordinator"
    """
    SHOW move_task
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """
  
  Scenario: REDISTRIBUTE KEY RANGE works when invoked from router
    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r2 ADDRESS regress_router_2:7000
    DROP KEY RANGE krid1;
    DROP KEY RANGE krid2;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
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
    When I run SQL on host "router-admin" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 2000;
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
    When I run SQL on host "router-admin"
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
    When I run SQL on host "router2-admin"
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
