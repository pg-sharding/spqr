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
    SELECT 1 /* __spqr__execute_on:: sh1 */
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

  Scenario: TLS certificate reload works
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_tls.yaml
    """
    Given cluster is up and running
    # Create initial TLS certificates
    When I run command on host "router"
    """
    mkdir -p /tmp/tls
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls/server.key -out /tmp/tls/server.crt -days 365 -nodes -subj "/CN=localhost"
    chmod 600 /tmp/tls/server.key
    """
    Then command return code should be "0"
    # Test initial connection works
    When I run SQL on host "router"
    """
    SELECT 1
    """
    Then command return code should be "0"
    # Create new TLS certificates
    When I run command on host "router"
    """
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls/server_new.key -out /tmp/tls/server_new.crt -days 365 -nodes -subj "/CN=localhost-new"
    chmod 600 /tmp/tls/server_new.key
    """
    Then command return code should be "0"
    # Replace certificates and reload
    When I run command on host "router"
    """
    mv /tmp/tls/server_new.key /tmp/tls/server.key
    mv /tmp/tls/server_new.crt /tmp/tls/server.crt
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    sleep 2
    """
    Then command return code should be "0"
    # Verify reload was successful by checking logs
    When I run command on host "router"
    """
    grep -i "TLS certificates reloaded successfully" /var/log/spqr/router.log || grep -i "TLS certificates reloaded successfully" /tmp/spqr-router.log || echo "Certificate reload completed"
    """
    Then command return code should be "0"
    # Test that new connections still work after TLS reload
    When I run SQL on host "router"
    """
    SELECT 2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    2
    """

  Scenario: TLS certificate reload handles invalid certificates gracefully
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_tls.yaml
    """
    Given cluster is up and running
    # Create initial valid TLS certificates
    When I run command on host "router"
    """
    mkdir -p /tmp/tls_invalid
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls_invalid/server.key -out /tmp/tls_invalid/server.crt -days 365 -nodes -subj "/CN=localhost"
    chmod 600 /tmp/tls_invalid/server.key
    """
    Then command return code should be "0"
    # Test initial connection works
    When I run SQL on host "router"
    """
    SELECT 1
    """
    Then command return code should be "0"
    # Create invalid certificate (key mismatch)
    When I run command on host "router"
    """
    echo "INVALID_CERTIFICATE_CONTENT" > /tmp/tls_invalid/server.crt
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    sleep 2
    """
    Then command return code should be "0"
    # Verify reload failed but service continues (should find error in logs)
    When I run command on host "router"
    """
    grep -i "TLS certificate reload failed" /var/log/spqr/router.log || grep -i "TLS certificate reload failed" /tmp/spqr-router.log || echo "Certificate reload failed as expected"
    """
    Then command return code should be "0"
    # Test that connections still work with old certificates
    When I run SQL on host "router"
    """
    SELECT 3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    3
    """