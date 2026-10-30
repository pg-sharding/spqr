Feature: User-facing QDB errors
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

  Scenario: A missing unique index is reported by the coordinator and router consoles
    When I run SQL on host "coordinator"
    """
    DROP UNIQUE INDEX missing_index;
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match json_exactly
    """
    "ERROR: unique index \"missing_index\" not found (SQLSTATE SPQRN)"
    """

    When I run SQL on host "router-admin"
    """
    DROP UNIQUE INDEX missing_index;
    """
    Then command return code should be "1"
    And SQL error on host "router-admin" should match json_exactly
    """
    "ERROR: unique index \"missing_index\" not found (SQLSTATE SPQRN)"
    """
