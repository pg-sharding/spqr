Feature: Basic test

  Scenario: SELECT 1 works
    Given cluster is up and running
    When I run SQL on host "shard1"
    """
    SELECT 1
    """
    Then command return code should be 0
    And command output should match
    """
    1
    """
    When I run SQL on host "router"
    """
    SELECT 1
    """
    Then command return code should be 0
    And command output should match
    """
    1
    1
    """