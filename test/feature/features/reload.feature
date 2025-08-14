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
    Given cluster is up and running
    # Create TLS certificates and update router config
    When I run command on host "router"
    """
    mkdir -p /tmp/tls
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls/server.key -out /tmp/tls/server.crt -days 365 -nodes -subj "/CN=localhost"
    chmod 600 /tmp/tls/server.key
    # Backup original config and add TLS section
    cp $ROUTER_CONFIG $ROUTER_CONFIG.backup
    cat >> $ROUTER_CONFIG << EOF
frontend_tls:
  sslmode: require
  cert_file: /tmp/tls/server.crt
  key_file: /tmp/tls/server.key
EOF
    # Restart router to apply TLS config
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    sleep 3
    """
    Then command return code should be "0"
    # Test that connection still works
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

  Scenario: TLS certificate reload handles expired certificates gracefully
    Given cluster is up and running
    # Create initial valid TLS certificates and configure TLS
    When I run command on host "router"
    """
    mkdir -p /tmp/tls_expired
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls_expired/server.key -out /tmp/tls_expired/server.crt -days 365 -nodes -subj "/CN=localhost"
    chmod 600 /tmp/tls_expired/server.key
    # Backup and update config
    cp $ROUTER_CONFIG $ROUTER_CONFIG.backup2
    cat >> $ROUTER_CONFIG << EOF
frontend_tls:
  sslmode: require
  cert_file: /tmp/tls_expired/server.crt
  key_file: /tmp/tls_expired/server.key
EOF
    # Apply TLS config
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    sleep 3
    """
    Then command return code should be "0"
    # Test initial connection works
    When I run SQL on host "router"
    """
    SELECT 1
    """
    Then command return code should be "0"
    # Create expired certificate (valid format but expired in the past) and trigger reload
    When I run command on host "router"
    """
    # Generate a certificate that was valid for one day starting May 7, 2000
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls_expired/server_expired.key -out /tmp/tls_expired/server_expired.crt -not_before 20000507120000Z -not_after 20000508120000Z -nodes -subj "/CN=localhost-expired"
    chmod 600 /tmp/tls_expired/server_expired.key
    # Replace the current certificates with expired ones
    mv /tmp/tls_expired/server_expired.key /tmp/tls_expired/server.key
    mv /tmp/tls_expired/server_expired.crt /tmp/tls_expired/server.crt
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    sleep 2
    """
    Then command return code should be "0"
    # Test that connections fail with expired certificates
    When I run SQL on host "router"
    """
    SELECT 3
    """
    Then command return code should be "1"
    And SQL error on host "router" should match regexp
    """
    certificate.*expired|TLS.*error|SSL.*error
    """