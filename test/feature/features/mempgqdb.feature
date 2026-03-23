Feature: MemQDB with PG dc state keeper test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    CREATE SHARD sh1 WITH HOSTS "spqr_shard_1:6432","spqr_shard_1_replica:6432";
    CREATE SHARD sh2 WITH HOSTS "spqr_shard_2:6432","spqr_shard_2_replica:6432";
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

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
