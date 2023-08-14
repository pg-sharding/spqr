Feature: Coordinator test
  Background:
    Given cluster is up and running
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE SHARDING RULE r1 COLUMN id;
    CREATE KEY RANGE krid1 FROM 0 TO 11 ROUTE TO sh1;
    CREATE KEY RANGE krid2 FROM 11 TO 31 ROUTE TO sh2;
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
    router with id r1 already exists
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
    Error while dialing
    """

  Scenario: Unregister router with invalid id fails
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r2
    """
    Then command return code should be "0"

  Scenario: Router synchronization after registration works
    Given I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1;
    REGISTER ROUTER r1
    """
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules
    """
    Then SQL result should match json_exactly
    """
    [{
      "Columns":"id",
      "Hash Function":"x->x",
      "Sharding Rule ID":"r1",
      "Table Name":"*"
    }]
    """

    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then SQL result should match json_exactly
    """
    [{
      "Key range ID":"krid1",
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Upper bound":"11"
    },
    {
      "Key range ID":"krid2",
      "Lower bound":"11",
      "Shard ID":"sh2",
      "Upper bound":"31"
    }]
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
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Upper bound":"5"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Lower bound":"5",
      "Shard ID":"sh1",
      "Upper bound":"11"
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
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Upper bound":"5"
    }]
    """
    And SQL result should match json
    """
    [{
      "Key range ID":"krid3",
      "Lower bound":"5",
      "Shard ID":"sh1",
      "Upper bound":"11"
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
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Upper bound":"11"
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
      "Lower bound":"0",
      "Shard ID":"sh1",
      "Upper bound":"11"
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
    CREATE KEY RANGE krid3 FROM 100 TO 1001 ROUTE TO sh1;
    UNITE KEY RANGE krid1 WITH krid3
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to unite not adjacent key ranges
    """

  Scenario: Unite in reverse order works
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 TO 40 ROUTE TO sh2;
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
      "Key range ID":"krid2",
      "Lower bound":"11",
      "Shard ID":"sh2",
      "Upper bound":"40"
    }]
    """

  Scenario: Unite key ranges routing different shards fails
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 TO 40 ROUTE TO sh1;
    UNITE KEY RANGE krid2 WITH krid3
    """
    Then SQL error on host "coordinator" should match regexp
    """
    failed to unite key ranges routing different shards
    """

  Scenario: Split key range by bound out of range fails
    #
    # Check we cannot split by bound greater than upper bound
    #
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid2 BY 40
    """
    Then SQL error on host "coordinator" should match regexp
    """
    bound is out of key range
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
    # Check we cannot split by right end of open interval
    #
    When I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid2 BY 31
    """
    Then SQL error on host "coordinator" should match regexp
    """
    bound is out of key range
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
    ADD KEY RANGE krid3 FROM 31 TO 40 ROUTE TO sh1
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
    CREATE KEY RANGE krid3 FROM 31 TO 40 ROUTE to sh1;
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
    When I run SQL on host "coordinator"
    """
    CREATE KEY RANGE krid3 FROM 31 TO 40 ROUTE TO sh1
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
      "Lower bound":"31",
      "Shard ID":"sh1",
      "Upper bound":"40"
    }]
    """

  Scenario: Add intersecting key range fails
    #
    # Create test key range
    #
    When I run SQL on host "coordinator"
    """
    ADD KEY RANGE krid3 FROM 100 TO 110 ROUTE TO sh1
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    ADD KEY RANGE krid4 FROM 90 TO 105 ROUTE TO sh1
    """
    Then SQL error on host "coordinator" should match regexp
    """
    key range krid4 intersects with krid3 present in qdb
    """

    When I run SQL on host "coordinator"
    """
    ADD KEY RANGE krid4 FROM 105 TO 115 ROUTE TO sh1
    """
    Then SQL error on host "coordinator" should match regexp
    """
    key range krid4 intersects with krid3 present in qdb
    """

    When I run SQL on host "coordinator"
    """
    ADD KEY RANGE krid4 FROM 102 TO 108 ROUTE TO sh1
    """
    Then SQL error on host "coordinator" should match regexp
    """
    key range krid4 intersects with krid3 present in qdb
    """

    When I run SQL on host "coordinator"
    """
    ADD KEY RANGE krid4 FROM 90 TO 120 ROUTE TO sh1
    """
    Then SQL error on host "coordinator" should match regexp
    """
    key range krid4 intersects with krid3 present in qdb
    """