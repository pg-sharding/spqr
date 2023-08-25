Feature: SCRAM auth test

  Scenario: Backend auth workd
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_scram_backend.yaml
    """
    Given cluster is up and running
    When I run command on host "shard1"
    """
    cat >> /var/lib/postgresql/13/main/pg_hba.conf <<-EOF
    host    all             regress             0.0.0.0/0            sram-sha-256
    EOF
    service postgresql reload
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