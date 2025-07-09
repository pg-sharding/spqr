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

  Scenario: Reference table sync with new shard
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

    # Step 1: Create reference table on initial shards (sh1, sh2)
    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE sync_test ON sh1, sh2;
    """
    Then command return code should be "0"

    # Create the table and insert test data
    When I execute SQL on host "router"
    """
    CREATE TABLE sync_test(id int, data text);
    INSERT INTO sync_test (id, data) VALUES(1, 'data1')  /* __spqr__engine_v2: true */;
    INSERT INTO sync_test (id, data) VALUES(2, 'data2')  /* __spqr__engine_v2: true */;
    INSERT INTO sync_test (id, data) VALUES(3, 'data3')  /* __spqr__engine_v2: true */;
    """
    Then command return code should be "0"

    # Step 2: Check if table exists and has data on sh1
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh1; 
    SELECT COUNT(*) as row_count FROM sync_test;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "row_count": 3
        }
    ]
    """

    # Step 2: Check if table exists and has data on sh2
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    SELECT COUNT(*) as row_count FROM sync_test;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "row_count": 3
        }
    ]
    """

    # Step 3: Add a new shard (sh3)
    When I run SQL on host "coordinator"
    """
    ADD SHARD sh3;
    """
    Then command return code should be "0"

    # Step 4: Check that reference table does NOT exist on sh3 (should fail)
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh3; 
    SELECT COUNT(*) as row_count FROM sync_test;
    """
    Then command return code should be "1"

    # Step 5: Sync reference tables to the new shard
    When I run SQL on host "coordinator"
    """
    SYNC REFERENCE TABLES ON sh3;
    """
    Then command return code should be "0"

    # Step 6: Check that table now exists and has data on sh3
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh3; 
    SELECT COUNT(*) as row_count FROM sync_test;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "row_count": 3
        }
    ]
    """

    # Verify the actual data is correct on sh3
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh3; 
    SELECT id, data FROM sync_test ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 1,
            "data": "data1"
        },
        {
            "id": 2,
            "data": "data2"
        },
        {
            "id": 3,
            "data": "data3"
        }
    ]
    """
