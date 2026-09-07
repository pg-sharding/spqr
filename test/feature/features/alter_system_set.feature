Feature: ALTER SYSTEM SET generic GUC

  Scenario: ALTER SYSTEM SET writes to autoconf file and persists across restart
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_autoconf.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    ALTER SYSTEM SET __spqr__maintain_params = 'on';
    """
    Then command return code should be "0"
    And file "/spqr/test/feature/conf/spqr.autoconf" on host "router" should match regexp
    """
    __spqr__maintain_params = on
    """

    # verify immediate value change
    When I run SQL on host "router"
    """
    SHOW __spqr__maintain_params;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "maintain params": "true"
      }
    ]
    """
    
    When host "router" is stopped
    And host "router" is started
    # verify value after restart
    When I run SQL on host "router"
    """
    SHOW __spqr__maintain_params;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "maintain params": "true"
      }
    ]
    """
