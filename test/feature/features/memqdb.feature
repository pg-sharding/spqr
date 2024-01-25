Feature: MemQDB save state into a file

  Scenario: Sharding rules restored
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running

    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """
  
    When I execute SQL on host "router-admin"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 TABLE test COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"id",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule1",
          "Table Name":"*"
      },
      {
          "Columns":"idx",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"test"
      },
      {
          "Columns":"idy",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """
  
  Scenario: backup is empty after DROP SHARDING RULE ALL
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """
    When I execute SQL on host "router-admin"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 TABLE test COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    DROP SHARDING RULE ALL;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """

  Scenario: Sharding rules initilized on startup without backups
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_initsql.yaml
    """
    Given cluster is up and running
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"id",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule1",
          "Table Name":"*"
      },
      {
          "Columns":"idx",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"test"
      },
      {
          "Columns":"idy",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """

  Scenario: Sharding rules initilized on startup even with backups
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup_and_initsql.yaml
    """
    Given cluster is up and running
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"id",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule1",
          "Table Name":"*"
      },
      {
          "Columns":"idx",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"test"
      },
      {
          "Columns":"idy",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """

  Scenario: Key ranges restored
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Key range ID": "krid1",
        "Distribution ID":"default",
        "Lower bound": "1",
        "Shard ID": "sh1"
      },
      {
        "Key range ID": "krid2",
        "Distribution ID":"default",
        "Lower bound": "11",
        "Shard ID": "sh1"
      }
    ]
    """

  Scenario: Sharding rules restored after droping specific one
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    ADD SHARDING RULE rule1 COLUMNS id;
    ADD SHARDING RULE rule2 TABLE test COLUMNS idx;
    ADD SHARDING RULE rule3 COLUMNS idy;
    DROP SHARDING RULE rule1;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"idx",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"test"
      },
      {
          "Columns":"idy",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """

  Scenario: Sharding rules restored after droping specific one (init.sql ignored)
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup_and_initsql.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    DROP SHARDING RULE rule1;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW sharding_rules;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "Columns":"idx",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule2",
          "Table Name":"test"
      },
      {
          "Columns":"idy",
          "Distribution ID":"default",
          "Hash Function":"x->x",
          "Sharding Rule ID":"rule3",
          "Table Name":"*"
      }
    ]
    """

  Scenario: Unlock after restart
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I run SQL on host "router-admin"
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
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    UNLOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    unlocked key range with id krid1
    """

  Scenario: Sharding rules not initialized if init.sql file doesn't exists
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_fake_initsql_and_backup.yaml
    """
    Given cluster is failed up and running
    And file "/go/router.log" on host "router" should match regexp
    """
    fake_init\.sql: no such file or directory
    """
