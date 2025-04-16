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
    CREATE REFERENCE TABLE t AUTO INCREMENT id;
    SHOW sequences;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "name": "t_id"
        }
    ]
    """

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, name text);
    INSERT INTO t (name) VALUES('test1') /* __spqr__engine_v2: true */;
    INSERT INTO t (name) VALUES('test2') /* __spqr__engine_v2: true */;
    INSERT INTO t (name) VALUES('test3') /* __spqr__engine_v2: true */;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SELECT id,name FROM t;
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
