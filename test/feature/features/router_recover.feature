Feature: Router bootstrap test
  Scenario: Key range lock is synced to router
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "Key range ID": "krid1",
        "Shard ID": "sh1",
        "Distribution ID": "ds1",
        "Lower bound": "1",
        "Locked": "true"
    }]
    """
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "Key range ID": "krid1",
        "Shard ID": "sh1",
        "Distribution ID": "ds1",
        "Lower bound": "1",
        "Locked": "true"
    }]
    """
