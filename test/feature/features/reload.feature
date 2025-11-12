Feature: Config reloading works

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

    When I run SQL on host "router" as user "regress2"
    """
    SELECT 1 /* __spqr__execute_on: sh1 */
    """
    Then command return code should be "1"
    And SQL error on host "router" should match regexp
    """
    shard sh1: failed to find primary
    """
    # Edit config and reload spqr-router
    When I run command on host "router"
    """
    sed -i 's/12345679/12345678/g' $ROUTER_CONFIG
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    """
    Then command return code should be "0"
    When I run SQL on host "router" as user "regress2"
    """
    SELECT 1
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1
    """