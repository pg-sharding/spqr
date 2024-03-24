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
        "Distribution ID": "ds1",
        "Column types": "integer"
      },
      {
        "Distribution ID": "ds2",
        "Column types": "varchar"
      }
    ]
    """

  Scenario: Attached relations restored
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_backup.yaml
    """
    Given cluster is up and running
    When I execute SQL on host "router-admin"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE DISTRIBUTION ds2 COLUMN TYPES varchar;
    ALTER DISTRIBUTION ds1 ATTACH RELATION a DISTRIBUTION KEY a_id HASH FUNCTION MURMUR;
    ALTER DISTRIBUTION ds1 ATTACH RELATION b DISTRIBUTION KEY b_id;
    ALTER DISTRIBUTION ds2 ATTACH RELATION c DISTRIBUTION KEY c_id;
    """
    Then command return code should be "0"
    When host "router" is stopped
    And host "router" is started
    When I run SQL on host "router-admin"
    """
    SHOW relations;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Relation name": "a",
        "Distribution ID": "ds1",
        "Distribution key": "(\"a_id\", murmur)"
      },
      {
        "Relation name": "b",
        "Distribution ID": "ds1",
        "Distribution key": "(\"b_id\", identity)"
      },
      {
        "Relation name": "c",
        "Distribution ID": "ds2",
        "Distribution key": "(\"c_id\", identity)"
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
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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

  Scenario: Sharding is not initialized if init.sql file doesn't exists
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_fake_initsql_and_backup.yaml
    """
    Given cluster is failed up and running
    And file "/go/router.log" on host "router" should match regexp
    """
    fake_init\.sql: no such file or directory
    """
