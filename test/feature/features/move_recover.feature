Feature: Move recover test

  Scenario: Interrapted transaction continues
    Given cluster is up and running
    When I execute SQL on host "coordinator"
    """
    ADD SHARDING RULE r1 COLUMNS w_id;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2;
    """
    Then command return code should be "0"
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
    Given host "coordinator" is started
    When I execute SQL on host "coordinator"
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
    Given cluster is up and running
    When I execute SQL on host "coordinator"
    """
    ADD SHARDING RULE r1 COLUMNS w_id;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2;
    """
    Then command return code should be "0"
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
    Given host "coordinator" is started
    When I execute SQL on host "coordinator"
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

  Scenario: coordinator saves transaction to QDB
    Given cluster is up and running
    When I execute SQL on host "coordinator"
    """
    ADD SHARDING RULE r1 COLUMNS w_id;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
    ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2;
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
    When I execute SQL on host "coordinator"
    """
    MOVE KEY RANGE krid1 to sh2
    """
    Then command return code should be "0"
    And qdb should contain transaction "krid1"
    Given host "coordinator" is stopped
    Given host "coordinator" is started
    When I execute SQL on host "coordinator"
    """
    SHOW routers
    """
    Then command return code should be "0"
    And qdb should not contain transaction "krid1"
    
