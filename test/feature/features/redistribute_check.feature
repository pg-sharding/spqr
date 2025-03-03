Feature: Redistribution test
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_three_shards.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_three_shards.yaml
    """

    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    ADD SHARD sh1 WITH HOSTS 'postgresql://regress@spqr_shard_1:6432/regress';
    ADD SHARD sh2 WITH HOSTS 'postgresql://regress@spqr_shard_2:6432/regress';
    """
    Then command return code should be "0"

  Scenario: REDISTRIBUTE KEY RANGE CHECK works with correct config
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "0"

  Scenario: REDISTRIBUTE KEY RANGE CHECK detects missing extension
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    DROP extension IF EXISTS postgres_fdw;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    foreign-data wrapper \"postgres_fdw\" does not exist
    """

  Scenario: REDISTRIBUTE KEY RANGE CHECK checks password correctness
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"

    When I run command on host "shard2"
    """
    echo 'host all all all password' > /var/lib/postgresql/13/main/pg_hba.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard2" to respond

    When I run SQL on host "shard2"
    """
    ALTER USER regress PASSWORD '12345679';
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
