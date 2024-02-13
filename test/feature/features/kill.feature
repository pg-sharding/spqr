Feature: Kill client test

  Scenario: kill client in coordinator works
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW clients
    """
    Then we save response row "0" column "client_id"
    And hide "client_id" field
    Then SQL result should match json
    """
    [
      {
        "client_id":"**IGNORE**",
        "dbname":"regress",
        "router_address":"regress_router:7000",
        "server_id":"no backend connection",
        "user":"regress"
      }
    ]
    """
    # TODO KILL client in coordinator is failing with
    # ERROR: grpcConnectionIterator pop not implemented (SQLSTATE )
    # When I execute SQL on host "coordinator"
    # """
    # KILL client {{ .client_id }}
    # """
    # Then command return code should be "0"
    # When I run SQL on host "coordinator"
    # """
    # SHOW clients
    # """
    # Then SQL result should match json
    # """
    # []
    # """

  Scenario: kill client in router works
    Given cluster is up and running
    When I run SQL on host "router-admin"
    """
    SHOW clients
    """
    Then we save response row "0" column "client_id"
    And hide "client_id" field
    Then we save response row "0" column "shard_time_0.75"
    And hide "shard_time_0.75" field
    Then we save response row "0" column "router_time_0.75"
    And hide "router_time_0.75" field
    Then SQL result should match json
    """
    [
      {
        "client_id":"**IGNORE**",
        "dbname":"regress",
        "router_address":"local",
        "router_time_0.75":"**IGNORE**",
        "server_id":"no backend connection",
        "shard_time_0.75":"**IGNORE**",
        "user":"regress"
      }
    ]
    """
    When I execute SQL on host "router-admin"
    """
    KILL client {{ .client_id }}
    """
    Then command return code should be "0"
    When I run SQL on host "router-admin"
    """
    SHOW clients
    """
    Then SQL result should match json
    """
    []
    """
