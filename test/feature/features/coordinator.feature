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
    CREATE KEY RANGE krid1 FROM 0 TO 10 ROUTE TO sh1;
    CREATE KEY RANGE krid2 FROM 11 TO 30 ROUTE TO sh2;
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
    Then SQL result should match regexp
    """
    \[\]
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

  Scenario: Register 2 routers with same address
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

  Scenario: Register 2 routers with same id
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

  Scenario: Register router with invalid address
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r3 ADDRESS invalid_router::7000
    """
    Then SQL error on host "coordinator" should match regexp
    """
    Error while dialing
    """

  Scenario: Unregister router with invalid id
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r2
    """
    Then command return code should be "0"

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
    And SQL result should match regexp
    """
    "Key range ID":"krid1".*"Lower bound":"0".*"Upper bound":"5"
    """
    And SQL result should match regexp
    """
    "Key range ID":"krid3".*"Lower bound":"5".*"Upper bound":"10"
    """

    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "Key range ID":"krid1".*"Lower bound":"0".*"Upper bound":"5"
    """
    And SQL result should match regexp
    """
    "Key range ID":"krid3".*"Lower bound":"5".*"Upper bound":"10"
    """

    When I run SQL on host "coordinator"
    """
    UNITE KEY RANGE krid1 WITH krid3;
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "Key range ID":"krid1".*"Lower bound":"0".*"Upper bound":"10"
    """

    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "Key range ID":"krid1".*"Lower bound":"0".*"Upper bound":"10"
    """

  Scenario: Split/Unite locked key range
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
    And SQL result should match regexp
    """
    \[\]
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
    And SQL result should match regexp
    """
    "Key range ID":"krid3"
    """