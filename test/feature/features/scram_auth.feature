Feature: SCRAM auth test

  Scenario: Frontend auth works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_scram_frontend.yaml
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

  Scenario: Backend auth works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_scram_backend.yaml
    """
    Given cluster is up and running
    When I run command on host "shard1" with timeout "220" seconds
    """
    echo 'host all all all scram-sha-256' > /var/lib/postgresql/13/main/pg_hba.conf
    service postgresql reload
    for (( i = 0; i < 10; i++)); do
        if PGPASSWORD=12345678 psql -h spqr_shard_1 -p 6432 -d db1 -U user1 -c "SELECT 1"; then
            break
        else
            sleep 20
        fi
    done
    """
    Then command return code should be "0"
    When I run command on host "shard2" with timeout "220" seconds
    """
    echo 'host all all all scram-sha-256' > /var/lib/postgresql/13/main/pg_hba.conf
    service postgresql reload
    for (( i = 0; i < 10; i++)); do
        if PGPASSWORD=12345678 psql -h spqr_shard_2 -p 6432 -d db1 -U user1 -c "SELECT 1"; then
            break
        else
            sleep 20
        fi
    done
    """
    Then command return code should be "0"
    When I run SQL on host "router"
    """
    SELECT 1
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    1
    """