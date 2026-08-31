Feature: SPQR behind the Odyssey connection pooler

  # SPQR is deployed with Odyssey in front of the router, so the router is exercised
  # through a pooler rather than by direct client connections. These scenarios cover that
  # path. The Odyssey version comes from ODYSSEY_IMAGE (a public image, see the "odyssey"
  # service in docker-compose.yaml) and is varied by the compatibility matrix in CI, so
  # the scenarios themselves must stay version agnostic: no assertions on Odyssey log
  # format, console output or any tool that happens to be present in a particular image.
  # Every scenario also keeps talking to "router" directly, so that a failure through the
  # pooler can be told apart from a failure of SPQR itself.

  Scenario: Sharded reads and writes work through Odyssey with transaction pooling
    Given cluster environment is
    """
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=transaction
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t DISTRIBUTION KEY id FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, v text);
    """
    Then command return code should be "0"

    # writes issued through the pooler must reach the shard the key range points at
    When I execute SQL on host "odyssey"
    """
    INSERT INTO t (id, v) VALUES (1, 'to sh1');
    INSERT INTO t (id, v) VALUES (150, 'to sh2');
    """
    Then command return code should be "0"

    When I run SQL on host "odyssey"
    """
    SELECT v FROM t WHERE id = 1;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "to sh1"
    }]
    """

    When I run SQL on host "odyssey"
    """
    SELECT v FROM t WHERE id = 150;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "to sh2"
    }]
    """

    When I run SQL on host "shard1"
    """
    SELECT v FROM t;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "to sh1"
    }]
    """

    When I run SQL on host "shard2"
    """
    SELECT v FROM t;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "to sh2"
    }]
    """

  Scenario: Sharded reads and writes work through Odyssey with session pooling
    Given cluster environment is
    """
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=session
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t DISTRIBUTION KEY id FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, v text);
    """
    Then command return code should be "0"

    When I execute SQL on host "odyssey"
    """
    INSERT INTO t (id, v) VALUES (1, 'to sh1');
    INSERT INTO t (id, v) VALUES (150, 'to sh2');
    """
    Then command return code should be "0"

    When I run SQL on host "odyssey"
    """
    SELECT v FROM t ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "to sh1"
    },
    {
        "v": "to sh2"
    }]
    """

  Scenario: Multi statement transactions are committed and rolled back through Odyssey
    Given cluster environment is
    """
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=transaction
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t DISTRIBUTION KEY id FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, v text);
    """
    Then command return code should be "0"

    When I execute SQL on host "odyssey"
    """
    BEGIN;
    INSERT INTO t (id, v) VALUES (1, 'committed');
    COMMIT;
    """
    Then command return code should be "0"

    When I execute SQL on host "odyssey"
    """
    BEGIN;
    INSERT INTO t (id, v) VALUES (2, 'rolled back');
    ROLLBACK;
    """
    Then command return code should be "0"

    # the rolled back row must not be visible, neither through the pooler nor on the shard
    When I run SQL on host "odyssey"
    """
    SELECT v FROM t WHERE id IN (1, 2) ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "committed"
    }]
    """

    When I run SQL on host "shard1"
    """
    SELECT v FROM t ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "committed"
    }]
    """

  Scenario: SPQR errors reach the client through Odyssey
    Given cluster environment is
    """
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=transaction
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t DISTRIBUTION KEY id FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    # a routing error raised by SPQR must not be swallowed or rewritten by the pooler
    When I run SQL on host "odyssey"
    """
    SELECT * FROM relation_without_distribution;
    """
    Then command return code should be "1"
    And SQL error on host "odyssey" should match regexp
    """
    distribution for relation .*relation_without_distribution.* not found \(SQLSTATE SPQRN\)
    """

    # the same error, and the same wording, when SPQR is addressed directly
    When I run SQL on host "router"
    """
    SELECT * FROM relation_without_distribution;
    """
    Then command return code should be "1"
    And SQL error on host "router" should match regexp
    """
    distribution for relation .*relation_without_distribution.* not found \(SQLSTATE SPQRN\)
    """

    # the connection is still usable after the error
    When I run SQL on host "odyssey"
    """
    SELECT 1 AS alive;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "alive": 1
    }]
    """

  Scenario: Prepared statements work through Odyssey
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=transaction
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

    When I prepare SQL on host "odyssey"
    """
    INSERT INTO t (id, name) VALUES(1, 'prepared') /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run prepared SQL on host "odyssey"
    """
    INSERT INTO t (id, name) VALUES(1, 'prepared') /*__spqr__engine_v2: true*/
    """
    Then command return code should be "0"

    When I run SQL on host "odyssey"
    """
    SELECT name FROM t ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "name": "prepared"
    }]
    """

  Scenario: Writes resume through Odyssey after the router is restarted
    Given cluster environment is
    """
    COMPOSE_PROFILES=odyssey
    ODYSSEY_POOL_TYPE=transaction
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    And I wait for host "coordinator" to finish startup

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 (int);
    CREATE RELATION t DISTRIBUTION KEY id FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I execute SQL on host "router"
    """
    CREATE TABLE t(id int, v text);
    """
    Then command return code should be "0"

    When I execute SQL on host "odyssey"
    """
    INSERT INTO t (id, v) VALUES (1, 'before restart');
    """
    Then command return code should be "0"

    Given host "router" is stopped
    And host "router" is started
    And I wait for host "router" to respond

    # a restarted router comes up without metadata and is resynced by the coordinator;
    # that part is SPQR's own behaviour, what this scenario is about is what the pooler
    # does with its server connections afterwards
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1;
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

    # the pooler must rebuild its server pool instead of leaving writes stuck
    When I execute SQL on host "odyssey"
    """
    INSERT INTO t (id, v) VALUES (2, 'after restart');
    """
    Then command return code should be "0"

    When I run SQL on host "odyssey"
    """
    SELECT v FROM t ORDER BY id;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "v": "before restart"
    },
    {
        "v": "after restart"
    }]
    """
