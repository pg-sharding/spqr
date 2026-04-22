Feature: MemQDB with PG dc state keeper test 
  Scenario: 2pc transaction happy path
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"
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
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"
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

  Scenario: planned transaction is rejected
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

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
    When I run SQL on host "router-admin"
    """
    SHOW dcs_storage;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"storage": "sh1"}]
    """
    When I run SQL on host "router-admin"
    """
    ATTACH CONTROL POINT 2pc_decision_cp panic;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table t (id int primary key, dat varchar);
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into t (id, dat) values (1, 'd1');
    commit;
    """
    Then command return code should be "1"

    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 1
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "planned"
    }]
    """
    When host "router" is started
    When I run SQL on host "router"
    """
    SELECT __spqr__run_2pc_recover();
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard2"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "rejected"
    }]
    """

  Scenario: committing transaction is committed
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

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
    When I run SQL on host "router-admin"
    """
    SHOW dcs_storage;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"storage": "sh1"}]
    """
    When I run SQL on host "router-admin"
    """
    ATTACH CONTROL POINT 2pc_after_decision_cp panic;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table t (id int primary key, dat varchar);
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into t (id, dat) values (1, 'd1');
    commit;
    """
    Then command return code should be "1"

    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 1
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committing"
    }]
    """
    When host "router" is started
    When I run SQL on host "router"
    """
    SELECT __spqr__run_2pc_recover();
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard2"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committed"
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committed"
    }]
    """
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    select id from t;
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
    select id from t;
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

  Scenario: planned transaction is rejected with watchdog
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb_watchdog.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

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
    When I run SQL on host "router-admin"
    """
    SHOW dcs_storage;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"storage": "sh1"}]
    """
    When I run SQL on host "router-admin"
    """
    ATTACH CONTROL POINT 2pc_decision_cp panic;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table t (id int primary key, dat varchar);
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into t (id, dat) values (1, 'd1');
    commit;
    """
    Then command return code should be "1"

    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 1
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "planned"
    }]
    """
    When host "router" is started
    And we wait for "30" seconds
    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard2"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "rejected"
    }]
    """

  Scenario: committing transaction is committed
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb_watchdog.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

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
    When I run SQL on host "router-admin"
    """
    SHOW dcs_storage;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"storage": "sh1"}]
    """
    When I run SQL on host "router-admin"
    """
    ATTACH CONTROL POINT 2pc_after_decision_cp panic;
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SET __spqr__engine_v2 TO on;
    create table t (id int primary key, dat varchar);
    SET __spqr__commit_strategy TO '2pc';
    begin;
    insert into t (id, dat) values (1, 'd1');
    commit;
    """
    Then command return code should be "1"

    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 1
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committing"
    }]
    """
    When host "router" is started
    And we wait for "30" seconds
    When I run SQL on host "shard1"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard2"
    """
    SELECT count(gid) FROM pg_prepared_xacts
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "count": 0
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committed"
    }]
    """
    When I run SQL on host "shard1"
    """
    SELECT id, members, status FROM spqr_metadata.spqr_tx_status
    """
    Then command return code should be "0"
    And SQL result should match json_regexp
    """
    [{
      "id": ".*",
      "members": "{sh1,sh2}",
      "status": "committed"
    }]
    """
    When I run SQL on host "router"
    """
    set __spqr__execute_on to sh2; 
    select id from t;
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
    select id from t;
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

  Scenario: old transaction data is removed
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_mempgqdb.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO spqr_metadata.spqr_tx_status (id, members, status, updated_at) VALUES
    (
      'done_outdated',
      ARRAY['sh1', 'sh2'],
      'committed',
      now() - INTERVAL '2 years'
    ),
    (
      'rejected_outdated',
      ARRAY['sh1', 'sh2'],
      'rejected',
      now() - INTERVAL '2 years'
    ),
    (
      'done_not_outdated',
      ARRAY['sh1', 'sh2'],
      'committed',
      now()
    ),
    (
      'rejected_not_outdated',
      ARRAY['sh1', 'sh2'],
      'rejected',
      now()
    ),
    (
      'planned_outdated',
      ARRAY['sh1', 'sh2'],
      'planned',
      now() - INTERVAL '2 years'
    ),
    (
      'planned_not_outdated',
      ARRAY['sh1', 'sh2'],
      'planned',
      now()
    ),
    (
      'committing_outdated',
      ARRAY['sh1', 'sh2'],
      'committing',
      now() - INTERVAL '2 years'
    ),
    (
      'committing_not_outdated',
      ARRAY['sh1', 'sh2'],
      'committing',
      now()
    )
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SELECT __spqr__clean_outdated_2pc_data();
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "gid": "done_outdated"
      },
      {
        "gid": "rejected_outdated"
      }
    ]
    """
    When I run SQL on host "shard1"
    """
    SELECT id FROM spqr_metadata.spqr_tx_status;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "id": "done_not_outdated"
      },
      {
        "id": "rejected_not_outdated"
      },
      {
        "id": "planned_outdated"
      },
      {
        "id": "planned_not_outdated"
      },
      {
        "id": "committing_outdated"
      },
      {
        "id": "committing_not_outdated"
      }
    ]
    """
