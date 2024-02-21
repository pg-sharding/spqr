Feature: Move recover test
  Background:
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: Interrapted transaction continues
    When I record in qdb data transfer transaction with name "krid2"
    """
    {"to_shard": "sh1",
    "from_shard": "sh2",
    "from_transaction": "tx2", 
    "to_transaction": "tx1",
    "from_tx_status": "process",
    "to_tx_status": "commit"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    insert into xMove(w_id, s) values(11, '002');
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    BEGIN;
    DELETE FROM xMove WHERE w_id = 11;
    PREPARE TRANSACTION 'tx2';
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    002
    """ 
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

  Scenario: Interrapted transaction rollbacks
    When I record in qdb data transfer transaction with name "krid2"
    """
    {"to_shard": "sh1",
    "from_shard": "sh2",
    "from_transaction": "tx2", 
    "to_transaction": "tx1",
    "from_tx_status": "process",
    "to_tx_status": "process"
    }
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(1, '001');
    BEGIN;
    insert into xMove(w_id, s) values(11, '002');
    PREPARE TRANSACTION 'tx2';
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    002
    """ 
    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    insert into xMove(w_id, s) values(11, '002');
    BEGIN;
    DELETE FROM xMove WHERE w_id = 11;
    PREPARE TRANSACTION 'tx2';
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    002
    """ 
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
    001
    """
    And SQL result should not match regexp
    """
    002
    """
    When I run SQL on host "shard2"
    """
    SELECT * FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    002
    """

  Scenario: coordinator saves transaction to QDB and processes it on restart
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
    When I execute SQL on host "coordinator"
    """
    MOVE KEY RANGE krid1 to sh2
    """
    Then command return code should be "0"
    And qdb should contain transaction "krid1"
    Given host "coordinator" is stopped
    When I execute SQL on host "coordinator2"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And qdb should not contain transaction "krid1"
    
