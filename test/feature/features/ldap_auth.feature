Feature: LDAP auth test

 Scenario: Get error when wrong password
   Given cluster environment is
   """
   ROUTER_CONFIG=/spqr/test/feature/conf/router_with_ldap_frontend.yaml
   """
   Given cluster is up and running
   When I run command on host "router"
   """
   PGPASSWORD=wro psql -c "SELECT 1" -d regress -U regress -p 6432 -h localhost
   """
   Then command return code should be "1"