Feature: Coordinator show clints, pools and backend_connections
    Background:
        Given cluster is up and running
        When I execute SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS regress_router:7000;
        REGISTER ROUTER r2 ADDRESS regress_router_2:7000
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW routers
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        r1-regress_router:7000(.|\n)*r2-regress_router_2:7000
        """
        Given I execute SQL on host "router"
        """
        SELECT pg_sleep(5)
        """
        And I execute SQL on host "router2"
        """
        SELECT pg_sleep(5)
        """

    Scenario: empty answer when no routers
        When I run SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1;
        UNREGISTER ROUTER r2;
        SHOW routers
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        r1-regress_router:7000(.|\n)*r2-regress_router_2:7000
        """
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000|regress_router_2:7000
        """
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000|regress_router_2:7000
        """
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000|regress_router_2:7000
        """

    Scenario: show clients works
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (regress_router:7000(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (regress_router_2:7000(.|\n)*){2}
        """

    Scenario: show clients collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (regress_router:7000(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (regress_router_2:7000(.|\n)*){2}
        """
        When I execute SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000(.|\n)*
        """
        And SQL result should match regexp
        """
        (regress_router_2:7000(.|\n)*){2}
        """

    Scenario: show backend_connections works
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (spqr_shard_1:6432(.|\n)*sh1(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (spqr_shard_2:6432(.|\n)*sh2(.|\n)*){2}
        """

    Scenario: show backend_connections collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (spqr_shard_1:6432(.|\n)*sh1(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (spqr_shard_2:6432(.|\n)*sh2(.|\n)*){2}
        """
        When I execute SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000(.|\n)*
        """
        And SQL result should match regexp
        """
        (regress_router_2:7000(.|\n)*){2}
        """

    Scenario: show pools works
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (spqr_shard_1:6432(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (spqr_shard_2:6432(.|\n)*){2}
        """

    Scenario: show pools collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        (spqr_shard_1:6432(.|\n)*){2}
        """
        And SQL result should match regexp
        """
        (spqr_shard_2:6432(.|\n)*){2}
        """
        When I execute SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        regress_router:7000(.|\n)*
        """
        And SQL result should match regexp
        """
        (regress_router_2:7000(.|\n)*){2}
        """