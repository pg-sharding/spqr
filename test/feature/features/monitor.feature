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
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
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
    key range .krid1. does not belong to any move task
    """

  Scenario: spqr-monitor verify fails when there are keys on destination shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
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
    cannot unlock key range: in relation .xMove. 2 entries on source shard, 1 entries on destination shard
    """
  
  Scenario: spqr-monitor verify fails when there are no keys on source shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
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
    cannot unlock key range: in relation .xMove. 0 entries on source shard, 2 entries on destination shard
    """
  
  Scenario: spqr-monitor verify succeeds when there are no keys on destination shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor verify --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml --key-range krid1 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    ^$
    """
  
  Scenario: spqr-monitor verify succeeds when there are the same number of keys on both shards
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor verify --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml --key-range krid1 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    ^$
    """

  Scenario: spqr-monitor recover does nothing when no move task in progress
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    ^$
    """

  Scenario: spqr-monitor recover does nothing when task group did not fail
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "RUNNING",
      "msg":   "executed by ..."
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    ^$
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges WHERE key_range_id = 'krid1';
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "distribution_id": "ds1",
        "key_range_id": "krid1",
        "locked": "true",
        "lower_bound":"1", 
        "shard_id":"sh1"
      }
    ]
    """
    When I run SQL on host "coordinator"
    """
    SHOW task_groups;
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [
      {
        "batch_size": "0",
        "destination_key_range_id": "kr_to",
        "destination_shard_id": "sh2",
        "message": "executed by ...",
        "move_task_id": "2",
        "source_key_range_id": "krid1",
        "state": "RUNNING",
        "task_group_id": "tgid1"
      }
    ]
    """
  
  Scenario: spqr-monitor recover does nothing when no task group status
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    ^$
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges WHERE key_range_id = 'krid1';
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "distribution_id": "ds1",
        "key_range_id": "krid1",
        "locked": "true",
        "lower_bound":"1", 
        "shard_id":"sh1"
      }
    ]
    """
  
  Scenario: spqr-monitor recover does nothing when there are keys on the destination shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    CREATE TABLE xMove2(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "ERROR",
      "msg":   "some error"
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    key range not safe to unlock: cannot unlock key range: in relation .xMove. 2 entries on source shard, 1 entries on destination shard
    """
  
  Scenario: spqr-monitor recover does nothing when there are keys on the destination shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "ERROR",
      "msg":   "some error"
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    key range not safe to unlock: cannot unlock key range: in relation .xMove. 0 entries on source shard, 2 entries on destination shard
    """
  
  Scenario: spqr-monitor recover succeeds when there are no keys on the destination shard
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "ERROR",
      "msg":   "some error"
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    deleting task group .tgid1.\. source key range: .krid1., dest key range: .kr_to., state: .ERROR., error msg: .some error.
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges WHERE key_range_id = 'krid1';
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "distribution_id": "ds1",
        "key_range_id": "krid1",
        "locked": "false",
        "lower_bound":"1", 
        "shard_id":"sh1"
      }
    ]
    """
    When I run SQL on host "coordinator"
    """
    SHOW task_groups;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """
  
  Scenario: spqr-monitor recover succeeds when there are the same number of keys on both shards
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    SET __spqr__execute_on TO sh2;
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "ERROR",
      "msg":   "some error"
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    deleting task group .tgid1.\. source key range: .krid1., dest key range: .kr_to., state: .ERROR., error msg: .some error.
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges WHERE key_range_id = 'krid1';
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "distribution_id": "ds1",
        "key_range_id": "krid1",
        "locked": "false",
        "lower_bound":"1", 
        "shard_id":"sh1"
      }
    ]
    """
    When I run SQL on host "coordinator"
    """
    SHOW task_groups;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    []
    """

  Scenario: spqr-monitor recover dry run works
   When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1;
    """
    Then command return code should be "0"
    When I record in qdb move task group
    """
    {
      "id":            "tgid1",
      "shard_to_id":   "sh2",
      "kr_id_from":    "krid1",
      "kr_id_to":      "kr_to",
      "type":          1,
      "limit":         -1,
      "coeff":         0.75,
      "bound_rel":     "test",
      "total_keys":    200,
      "task":
      {
        "id":            "2",
        "kr_id_temp":    "krid1",
        "bound":         ["FAAAAAAAAAA="],
        "state":         0,
        "task_group_id": "tgid1"
      }
    }
    """
    Then command return code should be "0"
    When I record in qdb status of move task group "tgid1"
    """
    {
      "state": "ERROR",
      "msg":   "some error"
    }
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-monitor recover --etcd-addr regress_qdb_0_1:2379 -c /spqr/test/feature/conf/shard_data.yaml --dry-run 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
    key range to unlock: .krid1.
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges WHERE key_range_id = 'krid1';
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "distribution_id": "ds1",
        "key_range_id": "krid1",
        "locked": "true",
        "lower_bound":"1", 
        "shard_id":"sh1"
      }
    ]
    """
    When I run SQL on host "coordinator"
    """
    SHOW task_groups;
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [
      {
        "batch_size": "0",
        "destination_key_range_id": "kr_to",
        "destination_shard_id": "sh2",
        "message": "some error",
        "move_task_id": "2",
        "source_key_range_id": "krid1",
        "state": "ERROR",
        "task_group_id": "tgid1"
      }
    ]
    """
