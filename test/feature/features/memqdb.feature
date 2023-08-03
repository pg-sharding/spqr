Feature: MemQDB save state into a file

  Scenario: Sharding rules restored
    Given cluster is up and running
    When I execute SQL on host "spqr-console"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 TABLE test COLUMNS idx;
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
          "Table Name":"test"
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

  Scenario: Key ranges restored
    Given cluster is up and running
    When I execute SQL on host "spqr-console"
    """
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;
    """
    Then command return code should be "0"
    When I restart router
    When I run SQL on host "spqr-console"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Key range ID": "krid1",
        "Lower bound": "1",
        "Shard ID": "sh1",
        "Upper bound": "10"
      },
      {
        "Key range ID": "krid2",
        "Lower bound": "11",
        "Shard ID": "sh1",
        "Upper bound": "20"
      }
    ]
    """

  Scenario: Sharding rules restored after droping specific one
    Given cluster is up and running
    When I execute SQL on host "spqr-console"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    DROP SHARDING RULE rule1;
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

  Scenario: Unlock after restart
    Given cluster is up and running
    When I run SQL on host "spqr-console"
    """
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    lock key range with id krid1
    """
    When I restart router
    When I run SQL on host "spqr-console"
    """
    UNLOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    unlocked key range with id krid1
    """
