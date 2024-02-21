Feature: MemQDB save state into a file

  Scenario: Distributions restored
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE DISTRIBUTION ds2 COLUMN TYPES varchar;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid3 FROM a ROUTE TO sh1 FOR DISTRIBUTION ds2;
    ALTER DISTRIBUTION ds1 ATTACH RELATION a DISTRIBUTION KEY a_id;
    ALTER DISTRIBUTION ds1 ATTACH RELATION b DISTRIBUTION KEY b_id;
    ALTER DISTRIBUTION ds2 ATTACH RELATION c DISTRIBUTION KEY c_id;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW distributions;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Distribution ID": "ds1"
      },
      {
        "Distribution ID": "ds2"
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
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
        "Distribution ID":"ds1",
        "Lower bound": "1",
        "Shard ID": "sh1"
      },
      {
        "Key range ID": "krid2",
        "Distribution ID":"ds1",
        "Lower bound": "11",
        "Shard ID": "sh1"
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
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
