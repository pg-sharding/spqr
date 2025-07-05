Feature: Reference relation test
  Scenario: Basic ref relation DML
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
    CREATE REFERENCE TABLE t ON sh1, sh2;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, name text);
    INSERT INTO t (id, name) VALUES(1, 'test1') /* __spqr__engine_v2: true */;
    INSERT INTO t (id, name) VALUES(2, 'test2') /* __spqr__engine_v2: true */;
    INSERT INTO t (id, name) VALUES(3, 'test3') /* __spqr__engine_v2: true */;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SET __spqr__execute_on TO sh1;
    SELECT id, name FROM t ORDER BY id;
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
    When I execute SQL on host "router"
    """
    UPDATE t SET id = id+1;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SET __spqr__execute_on TO sh2;
    SELECT id, name FROM t2 ORDER BY id ;
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
