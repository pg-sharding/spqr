Feature: Coordinator test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer; 
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test(id int, name text)
    """
    Then command return code should be "0"

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
    REGISTER ROUTER r2 ADDRESS regress_router::7000;
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router r2-regress_router:7000
    """

  Scenario: Register 2 routers with same address fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r2 ADDRESS regress_router::7000
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
    router r1-regress_router:7000
    """

  Scenario: Register 2 routers with same id fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
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
    router r1-regress_router:7000
    """

  Scenario: Register router with invalid address fails
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r3 ADDRESS invalid_router::7000
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
      "show routers":"router r1-regress_router:7000",
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
    Given I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1;
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then SQL result should match json_exactly
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh1"
    },
    {
      "Key range ID":"krid2",
      "Distribution ID":"ds1",
      "Lower bound":"11",
      "Shard ID":"sh2"
    }]
    """

  Scenario: Add key range with the same id fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid1 FROM 50 TO 100 ROUTE TO sh1 FOR DISTRIBUTION ds1
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
    SELECT name FROM test WHERE id=5
    """
    Then SQL error on host "router" should match regexp
    """
    context deadline exceeded
    """

    Given I run SQL on host "coordinator"
    """
    UNLOCK KEY RANGE krid1
    """
    When I run SQL on host "router"
    """
    INSERT INTO test(id, name) VALUES(5, 'random_word');
    SELECT name FROM test WHERE id=5
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    random_word
    """

  Scenario: Split/Unite key range works
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid1 BY 5;
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh1"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"5",
      "Shard ID":"sh1"
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
      "Lower bound":"0",
      "Shard ID":"sh1"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Distribution ID":"ds1",
      "Lower bound":"5",
      "Shard ID":"sh1"
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
      "Lower bound":"0",
      "Shard ID":"sh1"
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
      "Lower bound":"0",
      "Shard ID":"sh1"
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

  Scenario: Unite non-adjacent key ranges fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 100 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid1 WITH krid3
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to unite non-adjacent key ranges
    """

  Scenario: Unite in reverse order works
    When I run SQL on host "coordinator"
    """
    DROP KEY RANGE krid3;
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid3 WITH krid2
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
      "Lower bound":"11",
      "Shard ID":"sh2"
    }]
    """

  Scenario: Unite key ranges routing different shards fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    UNITE KEY RANGE krid2 WITH krid3
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
    SPLIT KEY RANGE krid3 FROM krid1 BY 40
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
    SPLIT KEY RANGE krid3 FROM krid2 BY 11
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to split because bound equals lower of the key range
    """

    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid2 BY 11
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to split because bound equals lower of the key range
    """

  Scenario: Adding/dropping shards works
    When I run SQL on host "coordinator"
    """
    ADD SHARD sh1 WITH HOSTS spqr_shard_1::6432;
    ADD SHARD sh2 WITH HOSTS spqr_shard_2::6432;
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
        "listing data shards": "datashard with ID sh1"
      },
      {
        "listing data shards": "datashard with ID sh2"
      }
    ]
    """
    When I run SQL on host "coordinator"
    """
    DROP SHARD sh1;
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
        "listing data shards": "datashard with ID sh2"
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
    router r1-regress_router:7000
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
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 ROUTE to sh1 FOR DISTRIBUTION ds1
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    context deadline exceeded
    """

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
      "Shard ID":"sh1"
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
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
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
        "Column types": "integer"
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
      "Shard ID":"sh1"
    },
    {
      "Key range ID":"krid2",
      "Distribution ID":"ds1",
      "Lower bound":"11",
      "Shard ID":"sh2"
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
        "Distribution key": "(\"id\", identity)"
      }
    ]
    """
