Feature: Sequence test
  Scenario: Auto increment column
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE t ON shard1, shard2, shard3;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, name text);
    INSERT INTO t (name) VALUES(1, 'test1') /* __spqr__engine_v2: true */;
    INSERT INTO t (name) VALUES(2, 'test2') /* __spqr__engine_v2: true */;
    INSERT INTO t (name) VALUES(3, 'test3') /* __spqr__engine_v2: true */;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SELECT id, name FROM t ORDER BY id /* __spqr__execute_on: shard2 */;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 1,
            "name": "test1"
        },
        {
            "id": 2,
            "name": "test2"
        },
        {
            "id": 3,
            "name": "test3"
        }
    ]
    """
        When I run SQL on host "router"
    """
    SELECT id, name FROM t ORDER BY id /* __spqr__execute_on: shard4 */;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
    ]
    """
    When I execute SQL on host "router"
    """
    UPDATE t SET id = id+1;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SELECT id, name FROM t2 ORDER BY id /* __spqr__execute_on: shard3 */;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 2,
            "name": "test1"
        },
        {
            "id": 3,
            "name": "test2"
        },
        {
            "id": 4,
            "name": "test3"
        }
    ]
    """
