Feature: Config reloading works
  @pure_pg
  Scenario: Backend auth works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_incorrect_password.yaml
    """
    Given cluster is up and running
    When I run command on host "shard1"
    """
    psql -c "CREATE user regress2 WITH PASSWORD '12345678' LOGIN" -d postgres -U postgres -p 6432
    """
    Then command return code should be "0"
    When I run command on host "shard1"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" | grep 'var/lib')
    echo 'host all all all password' > $datadir/pg_hba.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond

    When I run command on host "router"
    """
    psql -h localhost -p 6432 -U regress2 -d regress -c 'SELECT 1' 2>&1
    """
    Then command return code should be "2"
    And command output should match regexp
    """
    password authentication failed for user "regress2"
    """
    When I run command on host "router"
    """
    sed -i 's/12345679/12345678/g' $ROUTER_CONFIG
    """
    Then command return code should be "0"
    When I reload configuration on host "router"
    When I run SQL on host "router" as user "regress2"
    """
    SELECT 1
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1
    """

  @pure_pg
  Scenario: Show notice messages reload works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_incorrect_password.yaml
    """
    Given cluster is up and running
    When I run command on host "router"
    """
    psql -h localhost -p 6432 -U regress -d regress -c "SELECT 1 /* __spqr__execute_on: sh1 */" 2>&1
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    NOTICE:.*send query to shard\(s\) : sh1
    """
    When I run command on host "router"
    """
    sed -i 's/show_notice_messages: true/show_notice_messages: false/g' $ROUTER_CONFIG
    """
    Then command return code should be "0"
    When I reload configuration on host "router"
    When I run command on host "router"
    """
    psql -h localhost -p 6432 -U regress -d regress -c "SELECT 1 /* __spqr__execute_on: sh1 */" 2>&1
    """
    Then command return code should be "0"
    And command output should not match regexp
    """
    NOTICE:.*send query to shard\(s\) : sh1
    """

  @pure_pg
  Scenario: Backend auth works with overwritten user in auth rules
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_auth_rules.yaml
    """
    Given cluster is up and running
    When I run command on host "shard1"
    """
    psql -c "CREATE user regress2 WITH PASSWORD '12345678' LOGIN" -d postgres -U postgres -p 6432
    """
    Then command return code should be "0"
    When I run command on host "shard1"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" | grep 'var/lib')
    echo 'host all all all password' > $datadir/pg_hba.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond

    When I run SQL on host "router" as user "regress3"
    """
    SELECT 1 /* __spqr__execute_on: sh1 */
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1
    """
