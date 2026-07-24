Feature: TLS for SPQR internal gRPC communication
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster_tls.yaml
    ROUTER_CONFIG_2=/spqr/test/feature/conf/router_cluster_tls.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_tls.yaml
    COORDINATOR_CONFIG_2=/spqr/test/feature/conf/coordinator2_tls.yaml
    ROUTER_COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_tls.yaml
    ROUTER_2_COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_tls.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

  Scenario: Router and coordinator exchange metadata over verified TLS
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

    When I run SQL on host "router-admin"
    """
    CREATE DISTRIBUTION grpc_tls_ds COLUMN TYPES integer;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    [
      {
        "distribution_id": "grpc_tls_ds",
        "column_types": "integer"
      }
    ]
    """
