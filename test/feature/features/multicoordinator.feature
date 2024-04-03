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
    REGISTER ROUTER r1 ADDRESS regress_router::7000;
    REGISTER ROUTER r2 ADDRESS regress_router_2::7000
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
    And I fail to run SQL on host "coordinator2"

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
    And I fail to run SQL on host "coordinator"