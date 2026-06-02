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
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid3 FROM 1000 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: monitor works with no corruptions
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor check --etcd-addr regress_qdb_0_1:2379 --file /tmp/report.txt -c /spqr/test/feature/conf/shard_data.yaml --tablesample-size 100
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    0;OK
    """

  Scenario: monitor works with corruptions
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor check --etcd-addr regress_qdb_0_1:2379 --file /tmp/report.txt -c /spqr/test/feature/conf/shard_data.yaml --tablesample-size 100
    """
    Then command return code should be "2"
    And command output should match regexp
    """
    2;corruption found, check "/tmp/report.txt" file
    """
