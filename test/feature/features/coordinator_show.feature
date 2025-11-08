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
        REGISTER ROUTER r1 ADDRESS "regress_router:7000";
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
        SELECT pg_sleep(1) /* __spqr__scatter_query: true */
        """
        And I execute SQL on host "router2"
        """
        SELECT pg_sleep(1) /* __spqr__scatter_query: true */
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

    Scenario: 'show backend_connections group by' works 
        When I run SQL on host "coordinator"
        """
        SHOW backend_connections group by hostname
        """
        Then command return code should be "0"
        And SQL result should match json_regexp
        """
        [
            {
                "hostname":"spqr_shard_1:6432",
                "count": ".*"
            }
        ]
        """
        And SQL result should match json_regexp
        """
        [
            {
                "hostname":"spqr_shard_2:6432",
                "count": ".*"
            }
        ]
        """

        When I run SQL on host "coordinator"
        """
        SHOW backend_connections group by user
        """
        Then command return code should be "0"
        And SQL result should match json_regexp
        """
        [
            {
                "user":"regress",
                "count": ".*"
            }
        ]
        """

        When I run SQL on host "coordinator"
        """
        SHOW backend_connections group by dbname
        """
        Then command return code should be "0"
        And SQL result should match json_regexp
        """
        [
            {
                "dbname":"regress",
                "count": ".*"
            }
        ]
        """

        When I run SQL on host "coordinator"
        """
        SHOW backend_connections group by user, dbname
        """
        Then command return code should be "0"
        And SQL result should match json_regexp
        """
        [
            {
                "user": "regress",
                "dbname":"regress",
                "count": ".*"
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
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
                "pool router":"",
                "pool host":"spqr_shard_2:6432",
                "pool usr":"regress",
                "queue residual size":"50",
                "used connections":"0"
            }
        ]
        """

    Scenario: Show task group
        When I execute SQL on host "coordinator"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE kr_from FROM 0 ROUTE TO sh1;
        CREATE KEY RANGE kr_to FROM 20 ROUTE TO sh1;
        """
        Then command return code should be "0"
        When I record in qdb move task group
        """
        {
            "id":            "tgid1",
            "shard_to_id":   "sh_to",
            "kr_id_from":    "kr_from",
            "kr_id_to":      "kr_to",
            "type":          1,
            "limit":         -1,
            "coeff":         0.75,
            "bound_rel":     "test",
            "total_keys":    200,
            "task":
            {
                "id":            "2",
                "kr_id_temp":    "temp_id",
                "bound":         ["FAAAAAAAAAA="],
                "state":         0
            }
        }
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator"
        """
        SHOW task_group
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "Task group ID":            "tgid1",
            "Destination shard ID":     "sh_to",
            "Source key range ID":      "kr_from",
            "Destination key range ID": "kr_to"
        }]
        """
        When I run SQL on host "coordinator"
        """
        SHOW move_task
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "Move task ID":             "2",
            "State":                    "PLANNED",
            "Bound":                    "10",
            "Temporary key range ID":   "temp_id"
        }]
        """