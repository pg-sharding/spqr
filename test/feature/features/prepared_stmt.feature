Feature: Prepared statement feature test
  Scenario: Basic ref relation prepared statement
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

    When I run SQL on host "coordinator"
    """
    CREATE REFERENCE TABLE t ON sh1, sh2;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t (id int, name text);
    """
    Then command return code should be "0"

    When I prepare SQL on host "router"
    """
    INSERT INTO t (id, name) VALUES(1, 'test1') /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run prepared SQL on host "router"
    """
    INSERT INTO t (id, name) VALUES(1, 'test1') /*__spqr__engine_v2: true*/
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
        }
    ]
    """

    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    SELECT id, name FROM t ORDER BY id ;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "id": 1,
            "name": "test1"
        }
    ]
    """

 Scenario: Distributed relation prepared statement with query rewriting
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

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES varchar hash;
    CREATE KEY RANGE krid2 FROM 3505849917 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION users DISTRIBUTION KEY user_id HASH FUNCTION MURMUR;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    create table users (user_id text, name text);
    """
    Then command return code should be "0"

    When I prepare SQL on host "router"
    """
    INSERT INTO users(user_id, name) VALUES ('user1', 'name1'), ('user2', 'name2') /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run prepared SQL on host "router"
    """
    INSERT INTO users(user_id, name) VALUES ('user1', 'name1'), ('user2', 'name2') /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    select user_id, name from users;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user1",
            "name": "name1"
        }
    ]
    """

    When I run SQL on host "shard2"
    """
    select user_id, name from users ;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user2",
            "name": "name2"
        }
    ]
    """

 Scenario: reference relation joined to distributed relation prepared statement with query rewriting
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

    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES varchar hash;
    CREATE KEY RANGE krid2 FROM 3505849917 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION users DISTRIBUTION KEY user_id HASH FUNCTION MURMUR;
    CREATE REFERENCE TABLE dict;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    create table users (user_id text, name text, dict_id int);
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    create table dict (dict_id int);
    """
    Then command return code should be "0"

    When I prepare SQL on host "router"
    """
    INSERT INTO users(user_id, name, dict_id) VALUES ('user1', 'name1', 1), ('user2', 'name2', 1), ('user1', 'test', 1), ('user2', 'test', 1) /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run prepared SQL on host "router"
    """
    INSERT INTO users(user_id, name, dict_id) VALUES ('user1', 'name1', 1), ('user2', 'name2', 1), ('user1', 'test', 1), ('user2', 'test', 1) /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    select user_id, name from users order by name;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user1",
            "name": "name1"
        },
        {
            "user_id": "user1",
            "name": "test"
        }
    ]
    """

    When I run SQL on host "shard2"
    """
    select user_id, name from users order by name;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user2",
            "name": "name2"
        },
        {
            "user_id": "user2",
            "name": "test"
        }
    ]
    """

    When I run SQL on host "router"
    """
    SELECT u.user_id, u.name, d.dict_id
    FROM users u
    LEFT OUTER JOIN dict d ON u.dict_id = d.dict_id
    WHERE u.name = 'test' /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user1",
            "name": "test",
            "dict_id": null
        },
        {
            "user_id": "user2",
            "name": "test",
            "dict_id": null
        }
    ]
    """

    When I run SQL on host "router"
    """
    SELECT u.user_id, u.name, d.dict_id
    FROM users u
    LEFT OUTER JOIN dict d ON u.dict_id = d.dict_id
    order by u.name /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
        {
            "user_id": "user1",
            "name": "name1",
            "dict_id": null
        },
        {
            "user_id": "user1",
            "name": "test",
            "dict_id": null
        },
        {
            "user_id": "user2",
            "name": "name2",
            "dict_id": null
        },
        {
            "user_id": "user2",
            "name": "test",
            "dict_id": null
        }
    ]
    """