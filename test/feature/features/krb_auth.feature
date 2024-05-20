Feature: GSS Kerberos 5 auth test

  Scenario: Frontend auth works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_gss_frontend.yaml
    """
    Given cluster is up and running
    When I run command on host "router"
    """
    echo psql | kinit tester@MY.EX;
    psql -c "SELECT 1" -d regress -U tester -p 6432 -h localhost
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    1
    """

 Scenario: Get error when not auth password
   Given cluster environment is
   """
   ROUTER_CONFIG=/spqr/test/feature/conf/router_with_ldap_frontend.yaml
   """
   Given cluster is up and running
   When I run command on host "router"
   """
   PGPASSWORD=wrongpassword psql -c "SELECT 1" -d regress -U tester -p 6432 -h localhost
   """
   Then command return code should be "2"