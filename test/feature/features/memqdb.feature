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
        "distribution_id": "ds1",
        "column_types": "integer",
        "default_shard": "not exists"
      },
      {
        "distribution_id": "ds2",
        "column_types": "varchar",
        "default_shard": "not exists"
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
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer hash;
    CREATE DISTRIBUTION ds2 COLUMN TYPES varchar;
    ALTER DISTRIBUTION ds1 ATTACH RELATION a DISTRIBUTION KEY a_id HASH FUNCTION MURMUR;
    ALTER DISTRIBUTION ds2 ATTACH RELATION b DISTRIBUTION KEY b_id;
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
        "relation_name": "a",
        "distribution_id": "ds1",
        "distribution_key": "(\"a_id\", murmur)",
        "schema_name": "public"
      },
      {
        "relation_name": "b",
        "distribution_id": "ds2",
        "distribution_key": "(\"b_id\", identity)",
        "schema_name": "public"
      },
      {
        "relation_name": "c",
        "distribution_id": "ds2",
        "distribution_key": "(\"c_id\", identity)",
        "schema_name": "public"
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
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
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
        "key_range_id": "krid1",
        "distribution_id":"ds1",
        "lower_bound": "1",
        "shard_id": "sh1",
        "locked":"false"
      },
      {
        "key_range_id": "krid2",
        "distribution_id":"ds1",
        "lower_bound": "11",
        "shard_id": "sh1",
        "locked":"false"
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
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    key range id -\\u003e krid1
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
    key range id -\\u003e krid1
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

  Scenario: Router crashes when coordinator init is given as well as init.sql
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_initsql_and_coordinator_init.yaml
    """
    And cluster is failed up and running
