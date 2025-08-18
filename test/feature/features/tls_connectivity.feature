Feature: TLS connectivity
  In order to ensure secure connections
  As a SPQR user
  I want to connect to router via TLS

  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_tls.yaml
    """
    Given cluster is up and running

  Scenario: Connect with TLS enabled
    When I connect to "router" with TLS enabled
    And I execute SQL on "router"
    """
    CREATE TABLE test_tls(id INT);
    """
    Then command return code should be "0"
    When I execute SQL on "router" 
    """
    INSERT INTO test_tls VALUES (1);
    """
    Then command return code should be "0"
    When I execute SQL on "router"
    """
    SELECT * FROM test_tls;
    """
    Then command return code should be "0"
    And SQL result should match json
    """
    [{"id":1}]
    """
