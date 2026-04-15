Feature: Test integration with spqrguard

Scenario: spqrguard is set up correctly
    #
    # Make host "coordinator" take control
    #
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
    CREATE REFERENCE TABLE rt;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE t(id int);
    CREATE TABLE rt(id int);
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    SELECT (name, enabled) FROM spqr_metadata.spqr_global_settings;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "row": "(42,t)"
        },
        {
            "row": "(69,t)"
        }
    ]
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
    When I run SQL on host "shard1"
    """
    SELECT (SELECT reloid FROM spqr_metadata.spqr_reference_relations) = 'rt'::regclass::oid as check;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "check": true
    }]
    """

Scenario: router can write in shard
    #
    # Make host "coordinator" take control
    #
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
    CREATE REFERENCE TABLE rt;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE t(id int);
    CREATE TABLE rt(id int);
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO t (id) VALUES (0);
    INSERT INTO rt (id) VALUES (0);
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
    When I run SQL on host "router"
    """
    SELECT * FROM rt;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "id": 0
    }]
    """

Scenario: external writes are blocked in shard
    #
    # Make host "coordinator" take control
    #
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
    CREATE REFERENCE TABLE rt;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE t(id int);
    CREATE TABLE rt(id int);
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    SET spqrguard.prevent_distributed_table_modify TO 'unset';
    INSERT INTO t (id) VALUES (0);
    """
    Then command return code should be "1"
    And SQL error on host "shard1" should match regexp
    """
    unable to modify SPQR distributed relation within read-only transaction
    """
    When I run SQL on host "shard1"
    """
    SET spqrguard.prevent_reference_table_modify TO 'unset';
    INSERT INTO rt (id) VALUES (0);
    """
    Then command return code should be "1"
    And SQL error on host "shard1" should match regexp
    """
    unable to modify SPQR reference relation within read-only transaction
    """

Scenario: installation works without spqrguard
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_no_spqrguard.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_no_spqrguard.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "shard1"
    """
    SET allow_system_table_mods = true;
    DROP EXTENSION spqrguard;
    """

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

