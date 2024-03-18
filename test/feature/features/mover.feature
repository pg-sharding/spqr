Feature: Mover test
  Background:
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
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: mover works
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    002
    """
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001
    """
    When I run command on host "coordinator"
    """
    /spqr/spqr-mover -from-shard-connstring postgresql://regress@spqr_shard_1:6432/regress -to-shard-connstring postgresql://regress@spqr_shard_2:6432/regress -key-range krid1 -etcd-addr regress_qdb_0_1:2379
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    .*002(.|\n)*001
    """
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    001
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    #
    #  Mover shouldn't alter key ranges
    #
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"krid1",
      "Distribution ID":"ds1",
      "Lower bound":"1",
      "Shard ID":"sh1"
    },
    {
      "Key range ID":"krid2",
      "Distribution ID":"ds1",
      "Lower bound":"11",
      "Shard ID":"sh2"
    }]
    """

  Scenario: mover works with many rows
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(2, '002');
    insert into xMove(w_id, s) values(3, '003');
    insert into xMove(w_id, s) values(4, '004');
    insert into xMove(w_id, s) values(5, '005');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    /spqr/spqr-mover -from-shard-connstring postgresql://regress@spqr_shard_1:6432/regress -to-shard-connstring postgresql://regress@spqr_shard_2:6432/regress -key-range krid1 -etcd-addr regress_qdb_0_1:2379
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    .*001(.|\n)*002(.|\n)*003(.|\n)*004(.|\n)*005
    """
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    .*001(.|\n)*002(.|\n)*003(.|\n)*004(.|\n)*005
    """

  Scenario: mover works with many tables
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    insert into xMove2(w_id, s) values(2, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    CREATE TABLE xMove2(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    /spqr/spqr-mover -from-shard-connstring postgresql://regress@spqr_shard_1:6432/regress -to-shard-connstring postgresql://regress@spqr_shard_2:6432/regress -key-range krid1 -etcd-addr regress_qdb_0_1:2379
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    002
    """
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    001
    """
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove2
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """

  Scenario: Move to incorrect dsn fails
    When I run command on host "coordinator"
    """
    /spqr/spqr-mover -from-shard-connstring postgresql://regress@spqr_shard_1:6432/regress -to-shard-connstring postgresql://regress@nonexistent:6432/regress -key-range krid1 -etcd-addr regress_qdb_0_1:2379
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    hostname resolving error
    """

  Scenario: Move non-existent key range fails
    When I run command on host "coordinator"
    """
    /spqr/spqr-mover -from-shard-connstring postgresql://regress@spqr_shard_1:6432/regress -to-shard-connstring postgresql://regress@spqr_shard_2:6432/regress -key-range krid3 -etcd-addr regress_qdb_0_1:2379
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    failed to fetch key range at /keyranges/krid3
    """
