Feature: spqr-redistributor test
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
    CREATE KEY RANGE krid2 FROM 1000 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: spqr-redistributor successfully creates task
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    SET __spqr__execute_on TO sh1;
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text data';
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    ATTACH CONTROL POINT copy_data_cp SLEEP 10000 SECONDS;
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "30" seconds
    """
    /spqr/spqr-redistributor generate-task --coordinator-addr regress_coordinator:7003 --etcd-addr regress_qdb_0_1:2379 --chunk-size 200 --batch-size 100 --key-range-id krid1 --shard-id sh2 --max-tasks 1 2&> output.txt
    """
    Then command return code should be "0"
    When I run command on host "coordinator"
    """
    cat output.txt
    """
    Then command output should match regexp
    """
.*splitting key range .* by 800
.*redistributing key range .*
    """
    When I run SQL on host "coordinator"
    """
    SHOW redistribute_tasks;
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{
        "destination_shard_id":"sh2",
        "batch_size":"100"
    }]
    """
