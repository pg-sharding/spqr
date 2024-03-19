Feature: Coordinator show clients, pools and backend_connections
    Background:
        #
        # Make host "coordinator" take control
        #
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

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
        SELECT pg_sleep(1)
        """
        And I execute SQL on host "router2"
        """
        SELECT pg_sleep(1)
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
        And SQL result should match json_exactly
        """
        []
        """
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        []
        """
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        []
        """

    Scenario: show clients works
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "router_address":"regress_router:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            }
        ]
        """

    Scenario: show clients collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW clients
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "router_address":"regress_router:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            }
        ]
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
        regress_router:7000
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            },
            {
                "dbname":"regress",
                "router_address":"regress_router_2:7000",
                "user":"regress"
            }
        ]
        """

    Scenario: show backend_connections works
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_2:6432",
                "router":"regress_router:7000",
                "shard key name":"sh2",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_2:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh2",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_1:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh1",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_1:6432",
                "router":"regress_router:7000",
                "shard key name":"sh1",
                "user":"regress"
            }
        ]
        """

    Scenario: show backend_connections collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_2:6432",
                "router":"regress_router:7000",
                "shard key name":"sh2",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_2:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh2",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_1:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh1",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_1:6432",
                "router":"regress_router:7000",
                "shard key name":"sh1",
                "user":"regress"
            }
        ]
        """
        When I execute SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        # regexp (.|\n)* is here because {2} searches strings straight
        ((spqr_shard_1:6432(.|\n)*){2})|((spqr_shard_2:6432(.|\n)*){2})
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_1:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh1",
                "user":"regress"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "dbname":"regress",
                "hostname":"spqr_shard_2:6432",
                "router":"regress_router_2:7000",
                "shard key name":"sh2",
                "user":"regress"
            }
        ]
        """

    Scenario: show pools works
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
         And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_1:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router",
                "pool host":"spqr_shard_1:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """

    Scenario: show pools collects data from 2 routers
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
         And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_1:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router",
                "pool host":"spqr_shard_1:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        When I execute SQL on host "coordinator"
        """
        UNREGISTER ROUTER r1
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW pools
        """
        Then command return code should be "0"
        And SQL result should not match regexp
        """
        ((spqr_shard_1:6432(.|\n)*){2})|((spqr_shard_2:6432(.|\n)*){2})
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_1:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """
        And SQL result should match json
        """
        [
            {
                "idle connections":"1",
                "pool db":"regress",
                "pool router":"regress_router_2",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """