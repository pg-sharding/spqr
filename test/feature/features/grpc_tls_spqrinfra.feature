Feature: TLS in SPQR Infra mode
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_with_coordinator_tls.yaml
    ROUTER_CONFIG_2=/spqr/test/feature/conf/router_with_coordinator_tls.yaml
    ROUTER_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator_tls.yaml
    ROUTER_2_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator_tls.yaml
    """
    Given cluster is up and running
    And host "router2" is stopped
    And host "coordinator" is stopped
    And host "coordinator2" is stopped
    And I wait for coordinator address on router "router-admin" to become "regress_router:7003"
    And host "router2" is started

  Scenario: A TLS-enabled embedded coordinator survives leader failover
    When I run SQL on host "router-admin"
    """
    CREATE DISTRIBUTION grpc_tls_infra_ds COLUMN TYPES integer;
    """
    Then command return code should be "0"

    When host "router" is stopped
    And I wait for coordinator address on router "router2-admin" to become "regress_router_2:7003"

    When I run SQL on host "router2-admin"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    [
      {
        "distribution_id": "grpc_tls_infra_ds",
        "column_types": "integer"
      }
    ]
    """
