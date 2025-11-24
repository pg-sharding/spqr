Feature: Reference relation test
  Background:
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
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

  Scenario: Basic ref relation DML
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
    set __spqr__execute_on to sh1; 
    SELECT id, name FROM t ORDER BY id ;
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
    set __spqr__execute_on to sh1; 
    SELECT id, name FROM t ORDER BY id;
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

  Scenario: 2pc transaction happy path
    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE test_table ON sh1, sh2;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table test_table (id int primary key, dat varchar);
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into test_table (id, dat) values (1, 'd1');
    commit;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    select id from test_table;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [ 
        {
            "id": 1
        }
    ]
    """

    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh1; 
    select id from test_table;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [ 
        {
            "id": 1
        }
    ]
    """
 
  Scenario: 2pc transaction fails
    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE test_table ON sh1, sh2;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table test_table (id int primary key, dat varchar);
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh1; 
    insert into test_table (id, dat) values (1, 'd1');
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into test_table (id, dat) values (1, 'd1') ;
    commit;
    """
    Then command return code should be "1"
    And SQL error on host "router" should match regexp
    """
    duplicate key value violates unique
    """

    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    select id from test_table;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [ ]
    """

    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh1; 
    delete from test_table;
    """
    Then command return code should be "0"


  Scenario: create reference relation on create table
    When I execute SQL on host "router"
    """
    set __spqr__auto_distribution=REPLICATED;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE test (id int, name text);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW relations;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "Relation name": "test",
        "Distribution ID": "REPLICATED",
        "Distribution key":"",
        "Schema name": "$search_path"
      }
    ]
    """

    When I run SQL on host "router"
    """
    set __spqr__auto_distribution=REPLICATED1;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    SPQR invalid distribution 'REPLICATED1' for hint __spqr__auto_distribution
    """

    When I run SQL on host "router"
    """
    SHOW __spqr__auto_distribution;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly 
    """
    [
      {
        "__spqr__auto_distribution": "REPLICATED"
      }
    ]
    """
  
  Scenario: Ref relation sync works
    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE t ON sh1;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, name text[]);
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    INSERT INTO t (id, name) VALUES(1, ARRAY[]::text[]);
    INSERT INTO t (id, name) VALUES(2, ARRAY[null]);
    INSERT INTO t (id, name) VALUES(3, ARRAY['one_value']);
    INSERT INTO t (id, name) VALUES(4, ARRAY['two', 'values']);
    """
    Then command return code should be "0"
    
    When I run SQL on host "coordinator"
    """
    SYNC REFERENCE TABLE t ON sh2;
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    SELECT * FROM t
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 1,
            "name": "{}"
        },
        {
            "id": 2,
            "name": "{NULL}"
        },
        {
            "id": 3,
            "name": "{one_value}"
        },
        {
            "id": 4,
            "name": "{two,values}"
        }
    ]
    """

