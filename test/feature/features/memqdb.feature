Feature: MemQDB save state into a file

  Scenario: Sharding rules restored
    Given cluster is up and running
    When I execute SQL on host "spqr-console"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    """
    Then command return code should be "0"
    When I restart router
    When I run SQL on host "spqr-console"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"id",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule1",
          "Table Name":"*"
      },
      {
          "Columns":"idx",  
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"*"
      },
      {
          "Columns":"idy",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """
  
  Scenario: Sharding rules not restored
    Given cluster is up and running
    When I execute SQL on host "spqr-console"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    DROP SHARDING RULE ALL;
    """
    Then command return code should be "0"
    When I restart router
    When I run SQL on host "spqr-console"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """