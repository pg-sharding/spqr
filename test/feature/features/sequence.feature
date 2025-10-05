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
    REGISTER ROUTER r1 ADDRESS regress_router:7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE t AUTO INCREMENT id;
    CREATE REFERENCE TABLE t2 AUTO INCREMENT id START 10;
    SHOW sequences;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "name": "t2_id",
            "value": "10"
        },
        {
            "name": "t_id",
            "value": "0"
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
    CREATE TABLE t2 (id int, name text);
    INSERT INTO t2 (name) VALUES('test1') /* __spqr__engine_v2: true */;
    INSERT INTO t2 (name) VALUES('test2') /* __spqr__engine_v2: true */;
    INSERT INTO t2 (name) VALUES('test3') /* __spqr__engine_v2: true */;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SELECT id, name FROM t2 ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 11,
            "name": "test1"
        },
        {
            "id": 12,
            "name": "test2"
        },
        {
            "id": 13,
            "name": "test3"
        }
    ]
    """

  Scenario: Auto increment column
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running


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
            "name": "t_id",
            "value": "0"
        }
    ]
    """

    When I run SQL on host "coordinator"
    """
    DROP REFERENCE RELATION t;
    SHOW sequences;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """