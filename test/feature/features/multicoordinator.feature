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
    REGISTER ROUTER r1 ADDRESS "regress_router:7000";
    REGISTER ROUTER r2 ADDRESS regress_router_2:7000
    """
    Then command return code should be "0"

  Scenario: Second coordinator awaits
    When I run SQL on host "coordinator"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router
    """
    When I run SQL on host "coordinator2"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    """
    Then SQL error on host "coordinator2" should match regexp
    """
    console is in read only mode
    """

    When I run SQL on host "coordinator2"
    """
    SHOW is_read_only;
    """
    Then SQL result should match regexp
    """
    true
    """
    When I run SQL on host "coordinator"
    """
    SHOW is_read_only;
    """
    Then SQL result should match regexp
    """
    false
    """

  Scenario: Second coordinator turns on when other is dead
    Given host "coordinator" is stopped
    When I run SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router
    """

  Scenario: first coordinator awaits after recovery
    Given host "coordinator" is stopped
    When I run SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    router
    """
    Given host "coordinator" is started
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    """
    Then SQL error on host "coordinator" should match regexp
    """
    console is in read only mode
    """

    When I run SQL on host "coordinator"
    """
    SHOW is_read_only;
    """
    Then SQL result should match regexp
    """
    true
    """
    When I run SQL on host "coordinator2"
    """
    SHOW is_read_only;
    """
    Then SQL result should match regexp
    """
    false
    """
