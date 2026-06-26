Feature: spqr-monitor test
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
    Then command return code should be "0"
    And command output should match regexp
    """
    2;corruption found, check "/tmp/report.txt" file
    """
    When I run command on host "coordinator" with timeout "30" seconds
    """
    cat /tmp/report.txt
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    Corruption found: row \[1 001\], rel "xMove" shard "sh2"
    """

  Scenario: monitor works with multiple relations 
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove2(w_id, s) values(1, '001');
    insert into xMove2(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor check --etcd-addr regress_qdb_0_1:2379 --file /tmp/report.txt -c /spqr/test/feature/conf/shard_data.yaml --tablesample-size 100
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    2;corruption found, check "/tmp/report.txt" file
    """
    When I run command on host "coordinator" with timeout "30" seconds
    """
    cat /tmp/report.txt
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    Corruption found: row \[1 001\], rel "xMove2" shard "sh2"
    """

  Scenario: monitor exists if state file found
    When I run command on host "coordinator" with timeout "30" seconds
    """
    touch /tmp/report.txt && /spqr/spqr-monitor check --etcd-addr regress_qdb_0_1:2379 --file /tmp/report.txt -c /spqr/test/feature/conf/shard_data.yaml --tablesample-size 100
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    2;corruption found, check "/tmp/report.txt" file
    """
    When I run command on host "coordinator" with timeout "30" seconds
    """
    cat /tmp/report.txt
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    ^$
    """
  
  Scenario: spqr-monitor verify fails when no move task in progress
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove2(w_id, s) values(1, '001');
    insert into xMove2(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor verify --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml --key-range krid1 2&> output.txt
    """
    Then command return code should be "1"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    key range .* does not belong to any move task
    """
