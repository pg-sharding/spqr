Feature: Move recover test
  Background:
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;
    ADD KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: Planned key range movement continues
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "PLANNED"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    When I run SQL on host "coordinator2"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "key_range_id":"krid1",
          "distribution_id":"ds1",
          "lower_bound":"1",
          "shard_id":"sh1",
          "locked":"false"
      },
      {
          "key_range_id":"krid2",
          "distribution_id":"ds1",
          "lower_bound":"11",
          "shard_id":"sh1",
          "locked":"false"
      }
    ]
    """
    And qdb should not contain transaction "krid2"
    And qdb should not contain key range moves

  Scenario: Started key range movement continues
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid2
    """
    Then command return code should be "0"
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "STARTED"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    And we wait for "5" seconds
    When I run SQL on host "coordinator2"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "key_range_id":"krid1",
          "distribution_id":"ds1",
          "lower_bound":"1",
          "shard_id":"sh1",
          "locked":"false"
      },
      {
          "key_range_id":"krid2",
          "distribution_id":"ds1",
          "lower_bound":"11",
          "shard_id":"sh1",
          "locked":"false"
      }
    ]
    """
    And qdb should not contain transaction "krid2"
    And qdb should not contain key range moves

  Scenario: Started key range movement continues with planned transaction
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid2
    """
    Then command return code should be "0"
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "STARTED"
    }
    """
    Then command return code should be "0"
    When I record in qdb data transfer transaction with name "krid2"
    """
    {
    "to_shard": "sh1",
    "from_shard": "sh2",
    "status": "planned"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    When I run SQL on host "coordinator2"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "key_range_id":"krid1",
          "distribution_id":"ds1",
          "lower_bound":"1",
          "shard_id":"sh1",
          "locked":"false"
      },
      {
          "key_range_id":"krid2",
          "distribution_id":"ds1",
          "lower_bound":"11",
          "shard_id":"sh1",
          "locked":"false"
      }
    ]
    """
    And qdb should not contain transaction "krid2"
    And qdb should not contain key range moves

  Scenario: Started key range movement continues with dataCopied transaction
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid2
    """
    Then command return code should be "0"
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "STARTED"
    }
    """
    Then command return code should be "0"
    When I record in qdb data transfer transaction with name "krid2"
    """
    {
    "to_shard": "sh1",
    "from_shard": "sh2",
    "status": "data_copied"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values (1, '001'), (11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    When I run SQL on host "coordinator2"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "key_range_id":"krid1",
          "distribution_id":"ds1",
          "lower_bound":"1",
          "shard_id":"sh1",
          "locked":"false"
      },
      {
          "key_range_id":"krid2",
          "distribution_id":"ds1",
          "lower_bound":"11",
          "shard_id":"sh1",
          "locked":"false"
      }
    ]
    """
    And qdb should not contain transaction "krid2"
    And qdb should not contain key range moves

  Scenario: Started key range movement continues with completed transaction
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid2
    """
    Then command return code should be "0"
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "STARTED"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values (1, '001'), (11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    And we wait for "5" seconds
    When I run SQL on host "coordinator2"
    """
    SHOW key_ranges
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
          "key_range_id":"krid1",
          "distribution_id":"ds1",
          "lower_bound":"1",
          "shard_id":"sh1",
          "locked":"false"
      },
      {
          "key_range_id":"krid2",
          "distribution_id":"ds1",
          "lower_bound":"11",
          "shard_id":"sh1",
          "locked":"false"
      }
    ]
    """
    And qdb should not contain transaction "krid2"
    And qdb should not contain key range moves

  Scenario: Completed key range movement continues
    When I execute SQL on host "coordinator"
    """
    LOCK KEY RANGE krid2
    """
    Then command return code should be "0"
    When I record in qdb key range move
    """
    {
    "move_id": "move1",
    "key_range_id": "krid2",
    "shard_id": "sh1",
    "status": "COMPLETE"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001'), (11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    # key range "krid2" is unlocked
    When I execute SQL on host "router"
    """
    INSERT INTO xMove (w_id, s) values (12, 'text')
    """
    Then command return code should be "0"
    And qdb should not contain key range moves

  Scenario: Planned transaction continues
    When I record in qdb data transfer transaction with name "krid2"
    """
    {
    "to_shard": "sh1",
    "from_shard": "sh2",
    "status": "planned"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    And qdb should not contain transaction "krid2"

  Scenario: DataCopied transaction continues
    When I record in qdb data transfer transaction with name "krid2"
    """
    {
    "to_shard": "sh1",
    "from_shard": "sh2",
    "status": "data_copied"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values (1, '001'), (11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    Given host "coordinator" is stopped
    And I wait for "30" seconds for all key range moves to finish
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    001(.|\n)*002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """
    And qdb should not contain transaction "krid2"
  
  Scenario: Move task group retry works
    When I record in qdb move task group
    """
    {
        "id":            "tgid1",
        "shard_to_id":   "sh2",
        "kr_id_from":    "krid1",
        "kr_id_to":      "krid2",
        "type":          1,
        "limit":         10,
        "coeff":         1,
        "batch_size":    5,
        "bound_rel":     "xMove",
        "total_keys":    0,
        "task":
        {
          "id":            "1",
          "bound":         ["FAAAAAAAAAA="],
          "state":         0,
          "kr_id_temp":    "krid_temp1",
          "task_group_id": "tgid1"
        }
    }
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "task_group_id":            "tgid1",
        "destination_shard_id":     "sh2",
        "source_key_range_id":      "krid1",
        "destination_key_range_id": "krid2",
        "batch_size":               "5",
        "move_task_id":             "1",
        "state":                    "PLANNED",
        "error":                    ""
    }]
    """
    When I run SQL on host "coordinator"
    """
    SHOW move_task
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "move_task_id":             "1",
        "state":                    "PLANNED",
        "bound":                    "10",
        "temporary_key_range_id":   "krid_temp1",
        "task_group_id":            "tgid1"
    }]
    """
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) SELECT generate_series(1, 10), 'sample data';
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) SELECT generate_series(11, 12), 'sample data';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "120" seconds
    """
    RETRY MOVE TASK GROUP tgid1
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    4
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    8
    """

    Scenario: Move task group graceful stop works
    When I record in qdb move task group
    """
    {
        "id":            "tgid1",
        "shard_to_id":   "sh2",
        "kr_id_from":    "krid1",
        "kr_id_to":      "krid2",
        "type":          1,
        "limit":         10,
        "coeff":         1,
        "batch_size":    5,
        "bound_rel":     "xMove",
        "total_keys":    0,
        "task":
        {
          "id":            "1",
          "bound":         ["FAAAAAAAAAA="],
          "state":         0,
          "kr_id_temp":    "krid_temp1",
          "task_group_id": "tgid1"
        }
    }
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
    """
    SHOW task_group
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "task_group_id":            "tgid1",
        "destination_shard_id":     "sh2",
        "source_key_range_id":      "krid1",
        "destination_key_range_id": "krid2",
        "batch_size":               "5",
        "move_task_id":              "1",
        "state":                    "PLANNED",
        "error":                    ""
    }]
    """
    When I run SQL on host "coordinator"
    """
    SHOW move_task
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
        "move_task_id":             "1",
        "state":                    "PLANNED",
        "bound":                    "10",
        "temporary_key_range_id":   "krid_temp1",
        "task_group_id":            "tgid1"
    }]
    """
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) SELECT generate_series(1, 10), 'sample data';
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) SELECT generate_series(11, 12), 'sample data';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "120" seconds
    """
    STOP MOVE TASK GROUP tgid1;
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "120" seconds
    """
    RETRY MOVE TASK GROUP tgid1;
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    move task stopped by STOP MOVE TASK GROUP command
    """
    And qdb should not contain transfer tasks
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    9
    """
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    3
    """