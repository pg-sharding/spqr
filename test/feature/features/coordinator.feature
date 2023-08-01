Feature: Coordinator test
  Background:
    Given cluster is up and running
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    CREATE SHARDING RULE r1 COLUMN id;
    CREATE KEY RANGE krid1 from 0 to 10 route to sh1;
    CREATE KEY RANGE krid2 from 11 to 30 route to sh2;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test(id int, name text)
    """
    Then command return code should be "0"

  Scenario: Lock/Unlock key range works
    Given I run SQL on host "coordinator"
    """
    LOCK KEY RANGE krid1
    """
    When I run SQL on host "router"
    """
    SELECT name FROM test WHERE id=5
    """
    Then SQL error on host "router" should match regexp
    """
    context deadline exceeded
    """

    Given I run SQL on host "coordinator"
    """
    UNLOCK KEY RANGE krid1
    """
    When I run SQL on host "router"
    """
    INSERT INTO test(id, name) VALUES(5, 'random_word');
    SELECT name FROM test WHERE id=5
    """
    Then SQL result should match regexp
    """
    random_word
    """

  Scenario: Split/Unite key range works
    Given I run SQL on host "coordinator"
    """
    SPLIT KEY RANGE krid3 FROM krid1 BY 5;
    LOCK KEY RANGE krid3;
    """
    And I run SQL on host "router"
    """
    SELECT name FROM test WHERE id=7
    """
    Then SQL error on host "router" should match regexp
    """
    context deadline exceeded
    """

    When I run SQL on host "router"
    """
    INSERT INTO test(id, name) VALUES(5, 'random_word');
    SELECT name FROM test WHERE id=5
    """
    Then SQL result should match regexp
    """
    random_word
    """

    Given I run SQL on host "coordinator"
    """
    UNLOCK KEY RANGE krid3
    """
    When I run SQL on host "coordinator"
    """
    UNITE KEY RANGE krid1 WITH krid3;
    LOCK KEY RANGE krid1;
    """
    And I run SQL on host "router"
    """
    SELECT name FROM test WHERE id=5
    """
    Then SQL error on host "router" should match regexp
    """
    context deadline exceeded
    """

    When I run SQL on host "router"
    """
    SELECT name FROM test WHERE id=6
    """
    Then SQL error on host "router" should match regexp
    """
    context deadline exceeded
    """

  #Scenario: Move key range works

  #Scneario: Coordinator can restart