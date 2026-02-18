Feature: Redistribution retries test
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_three_shards.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_three_shards.yaml
    """
    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    REGISTER ROUTER r2 ADDRESS "[regress_router_2]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: move task is retryable on planned stage
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
            "id":            "tgid1",
            "shard_to_id":   "sh2",
            "kr_id_from":    "kr1",
            "kr_id_to":      "kr_to",
            "type":          1,
            "limit":         -1,
            "coeff":         1,
            "batch_size":    100,
            "bound_rel":     "xMove",
            "total_keys":    0,
            "task":
            {
                "id":            "mt1",
                "kr_id_temp":    "kr_to",
                "bound":         ["FAAAAAAAAAA="],
                "state":         0,
                "task_group_id": "tgid1"
            }
        }
    """
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    RETRY TASK GROUP tgid1
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
  
  Scenario: move task is retryable on planned stage with split failed on routers
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr_to FROM 901 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
            "id":            "tgid1",
            "shard_to_id":   "sh2",
            "kr_id_from":    "kr1",
            "kr_id_to":      "kr_to",
            "type":          1,
            "limit":         -1,
            "coeff":         1,
            "batch_size":    100,
            "bound_rel":     "xMove",
            "total_keys":    0,
            "task":
            {
                "id":            "mt1",
                "kr_id_temp":    "kr_to",
                "bound":         ["ig4AAAAAAAAAAA=="],
                "state":         0,
                "task_group_id": "tgid1"
            }
        }
    """
    # simulate network fail on 2nd router
    When I run SQL on host "router2-admin"
    """
    DROP KEY RANGE ALL;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    RETRY TASK GROUP tgid1
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

  Scenario: move task is retryable on move stage with manual fix
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr_to FROM 801 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 800), 'sample text value' /* __spqr__execute_on: sh1 */;
    INSERT INTO xMove (w_id, s) SELECT generate_series(801, 999), 'sample text value' /* __spqr__execute_on: sh2 */;
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
            "id":            "tgid1",
            "shard_to_id":   "sh2",
            "kr_id_from":    "kr1",
            "kr_id_to":      "kr_to",
            "type":          1,
            "limit":         -1,
            "coeff":         1,
            "batch_size":    100,
            "bound_rel":     "xMove",
            "total_keys":    200,
            "task":
            {
                "id":            "mt1",
                "kr_id_temp":    "kr_temp",
                "bound":         ["ig4AAAAAAAAAAA=="],
                "state":         2,
                "task_group_id": "tgid1"
            }
        }
    """
     # simulate network fail on 2nd router
    When I run SQL on host "router2-admin"
    """
    DROP KEY RANGE ALL;
    CREATE KEY RANGE kr_to FROM 901 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr_tmp FROM 801 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    # currently such error cannot be retried automatically
    When I run SQL on host "coordinator"
    """
    RETRY TASK GROUP tgid1
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    no key range found at /keyranges/kr_temp
    """

    # move task can be finished manually
    When I run SQL on host "router2-admin"
    """
    UNITE KEY RANGE kr_to WITH kr_tmp;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    DROP MOVE TASK "mt1"
    """
    Then command return code should be "0"
    When I delete key "/task_group_locks/tgid1" from etcd
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    RETRY TASK GROUP tgid1
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    0
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1000
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """
    When I run SQL on host "router2-admin"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr_to",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh2",
      "locked":"false"
    }]
    """

