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
    echo 'host all all all password' > /var/lib/postgresql/13/main/pg_hba.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond
    When I fail to run SQL on host "router" as user "regress2"
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