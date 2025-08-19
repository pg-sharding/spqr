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

  Scenario: Connect with TLS works
    When I connect to "router" with TLS enabled
    When I run SQL on host "router"
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
    # Create TLS certificates and update the router config
    When I run command on host "router"
    """
    mkdir -p /tmp/tls_expired
    # Generate an expired certificate (expired yesterday)
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls_expired/server_expired.key -out /tmp/tls_expired/server_expired.crt -days 1 -nodes -subj "/CN=localhost-expired"
    chmod 600 /tmp/tls_expired/server_expired.key
    # Move the expired certificate to the expected location
    mv /tmp/tls_expired/server_expired.key /etc/spqr/ssl/server.key
    mv /tmp/tls_expired/server_expired.crt /etc/spqr/ssl/server.crt
    """
    Then command return code should be "0"
    # Restart the router to apply TLS config
    When host "router" is stopped
    And host "router" is started
    When I connect to "router" with TLS enabled
    When I run SQL on host "router"
    """
    SELECT 2
    """
    Then command return code should be "1"
    And SQL error on host "router" should match regexp
    """
    certificate.*expired|TLS.*error|SSL.*error
    """
    # Create a new valid TLS certificates
    When I run command on host "router"
    """
    mkdir -p /tmp/tls
    openssl req -x509 -newkey rsa:2048 -keyout /tmp/tls/server_new.key -out /tmp/tls/server_new.crt -days 365 -nodes -subj "/CN=localhost-new"
    chmod 600 /tmp/tls/server_new.key
    """
    Then command return code should be "0"
    # Replace certificates and reload
    When I run command on host "router"
    """
    mv /tmp/tls/server_new.key /etc/spqr/ssl/server.key
    mv /tmp/tls/server_new.crt /etc/spqr/ssl/server.crt
    """
    Then command return code should be "0"
    # Reload the router to apply new TLS certificates
    When I run command on host "router"
    """
    pkill -HUP spqr-router
    sleep 2
    """
    Then command return code should be "0"
    # Test that new connections works
    When I connect to "router" with TLS enabled
    When I run SQL on host "router"
    """
    SELECT 3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    3
    """

