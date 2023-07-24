Feature: Move test

  Scenario: MOVE KEY RANGE works
    Given cluster is up and running
    When I execute SQL on host "coordinator"
    """
    ADD SHARDING RULE r1 COLUMNS w_id;
    ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;
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
    """
    Then command return code should be "0"
    When I execute SQL on host "coordinator"
    """
    MOVE KEY RANGE krid1 to sh2
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