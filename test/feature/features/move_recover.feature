Feature: Move recover test
  Background:
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;
    ADD KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
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
          "Key range ID":"krid1",
          "Distribution ID":"ds1",
          "Lower bound":"1",
          "Shard ID":"sh1"
      },
      {
          "Key range ID":"krid2",
          "Distribution ID":"ds1",
          "Lower bound":"11",
          "Shard ID":"sh1"
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
          "Key range ID":"krid1",
          "Distribution ID":"ds1",
          "Lower bound":"1",
          "Shard ID":"sh1"
      },
      {
          "Key range ID":"krid2",
          "Distribution ID":"ds1",
          "Lower bound":"11",
          "Shard ID":"sh1"
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
          "Key range ID":"krid1",
          "Distribution ID":"ds1",
          "Lower bound":"1",
          "Shard ID":"sh1"
      },
      {
          "Key range ID":"krid2",
          "Distribution ID":"ds1",
          "Lower bound":"11",
          "Shard ID":"sh1"
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
          "Key range ID":"krid1",
          "Distribution ID":"ds1",
          "Lower bound":"1",
          "Shard ID":"sh1"
      },
      {
          "Key range ID":"krid2",
          "Distribution ID":"ds1",
          "Lower bound":"11",
          "Shard ID":"sh1"
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
          "Key range ID":"krid1",
          "Distribution ID":"ds1",
          "Lower bound":"1",
          "Shard ID":"sh1"
      },
      {
          "Key range ID":"krid2",
          "Distribution ID":"ds1",
          "Lower bound":"11",
          "Shard ID":"sh1"
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
