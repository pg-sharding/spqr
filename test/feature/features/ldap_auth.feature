Feature: LDAP auth test

  Scenario: LDAP auth works with one server
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_ldap_frontend.yaml
    """
    Given cluster is up and running
    When I run command on host "router"
    """
    PGPASSWORD=12345678 psql -c "SELECT 1" -d regress -U regress -p 6432 -h localhost
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    1
    """
  
  Scenario: LDAP auth works when one of servers is dead
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_ldap_frontend.yaml
    """
    Given cluster is up and running
    Given host "ldapserver" is stopped
    When I run command on host "router"
    """
    PGPASSWORD=12345678 psql -c "SELECT 1" -d regress -U regress -p 6432 -h localhost
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    1
    """

 Scenario: Get error when wrong password
   Given cluster environment is
   """
   ROUTER_CONFIG=/spqr/test/feature/conf/router_with_ldap_frontend.yaml
   """
   Given cluster is up and running
   When I run command on host "router"
   """
   PGPASSWORD=wrongpassword psql -c "SELECT 1" -d regress -U regress -p 6432 -h localhost
   """
   Then command return code should be "2"