Feature: Test integration with spqrguard
Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    ROUTER_CONFIG_2=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t (id);
    CREATE KEY RANGE kr0 FROM 0 ROUTE TO sh1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE t(id int)
    """
    Then command return code should be "0"

Scenario: spqrguard is set up correctly
    When I run SQL on host "shard1"
    """
    SELECT (name, enabled) FROM spqr_metadata.spqr_global_settings;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "row": "(42,t)"
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT (SELECT reloid FROM spqr_metadata.spqr_distributed_relations) = 't'::regclass::oid as check;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "check": true
    }]
    """

Scenario: router can write in shard
    When I run SQL on host "router"
    """
    INSERT INTO t (id) VALUES (0)
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SELECT * FROM t;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "id": 0
    }]
    """

Scenario: third party cannot write in shard
    When I run SQL on host "shard1"
    """
    INSERT INTO t (id) VALUES (0)
    """
    Then command return code should be "1"
    And SQL error on host "shard1" should match regexp
    """
    unable to modify SPQR distributed relation within read-only transaction
    """


