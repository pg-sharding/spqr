Feature: Router in cluster test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    ROUTER_CONFIG_2=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

  Scenario: Autoattach relation to distribution works
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    """
    Then command return code should be "0"

    When I run SQL on host "router-admin"
    """
    SHOW distributions;
    """
    Then SQL result should match json
    """
    [
        {
            "distribution_id":"ds1",
            "Column types":"integer"
        }
    ]
    """

    When I execute SQL on host "router"
    """
    CREATE TABLE d_zz (i int, j int) /* __spqr__auto_distribution: ds1, __spqr__distribution_key: j */;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW relations;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "relation_name": "d_zz",
        "distribution_id": "ds1",
        "distribution_key": "(\"j\", identity)",
        "schema_name": "$search_path"
      }
    ]
    """

    When I run SQL on host "router-admin"
    """
    SHOW relations;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [
      {
        "relation_name": "d_zz",
        "distribution_id": "ds1",
        "distribution_key": "(\"j\", identity)",
        "schema_name": "$search_path"
      }
    ]
    """