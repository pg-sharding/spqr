Feature: Proxy console
    Background:
        #
        # Run routers with coordinators
        # Stop all coordinators
        #
        Given cluster environment is
        """
        ROUTER_CONFIG=/spqr/test/feature/conf/router_with_coordinator.yaml
        COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator.yaml
        COORDINATOR_CONFIG_2=/spqr/test/feature/conf/router_coordinator_2.yaml
        """
        Given cluster is up and running
        And host "coordinator" is stopped
        And host "coordinator2" is stopped

        #
        # Make host "router" take control over coordinator
        #
        Given host "router2" is stopped
        And host "router2" is started

        When I run SQL on host "router-admin"
        """
        UNREGISTER ROUTER ALL;
        REGISTER ROUTER r1 ADDRESS regress_router::7000;
        REGISTER ROUTER r2 ADDRESS regress_router_2::7000;
        """
        Then command return code should be "0"

    Scenario: Add key_range/sharding_rule is executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE KEY RANGE krid1 FROM 0 TO 10 ROUTE TO sh1;
        CREATE SHARDING RULE r1 COLUMN id;
        """
        Then command return code should be "0"

        #
        # Check on first router
        #
        When I run SQL on host "router-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Dataspace ID":"default",
            "Lower bound":"0",
            "Shard ID":"sh1",
            "Upper bound":"deprecated"
        }]
        """
        When I run SQL on host "router-admin"
        """
        SHOW sharding_rules
        """
        Then SQL result should match json_exactly
        """
        [{
            "Columns":"id",
            "Dataspace ID":"default",
            "Hash Function":"x->x",
            "Sharding Rule ID":"r1",
            "Table Name":"*"
        }]
        """

        #
        # Check on second router
        #
        When I run SQL on host "router2-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Dataspace ID":"default",
            "Lower bound":"0",
            "Shard ID":"sh1",
            "Upper bound":"deprecated"
        }]
        """
        When I run SQL on host "router2-admin"
        """
        SHOW sharding_rules
        """
        Then SQL result should match json_exactly
        """
        [{
            "Columns":"id",
            "Dataspace ID":"default",
            "Hash Function":"x->x",
            "Sharding Rule ID":"r1",
            "Table Name":"*"
        }]
        """

    Scenario: Lock/Unlock key_range are executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
        CREATE SHARDING RULE r1 COLUMN id;
        """
        Then command return code should be "0"

        When I run SQL on host "router"
        """
        CREATE TABLE test(id int);
        """
        Then command return code should be "0"

        When I run SQL on host "router-admin"
        """
        LOCK KEY RANGE krid1
        """
        Then command return code should be "0"

        When I run SQL on host "router2"
        """
        SELECT * FROM test WHERE id=5;
        """
        Then SQL error on host "router2" should match regexp
        """
        context deadline exceeded
        """

        When I run SQL on host "router-admin"
        """
        UNLOCK KEY RANGE krid1
        """
        Then command return code should be "0"

        When I run SQL on host "router2"
        """
        SELECT * FROM test WHERE id=5;
        """
        Then command return code should be "0"

    Scenario: Split/Unite key_range are executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1;
        SPLIT KEY RANGE new_krid FROM krid1 BY 5;
        """
        Then command return code should be "0"

        When I run SQL on host "router2-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Dataspace ID":"default",
            "Lower bound":"0",
            "Shard ID":"sh1",
            "Upper bound":"deprecated"
        },
        {
            "Key range ID":"new_krid",
            "Dataspace ID":"default",
            "Lower bound":"5",
            "Shard ID":"sh1",
            "Upper bound":"deprecated"
        }]
        """

        When I run SQL on host "router-admin"
        """
        UNITE KEY RANGE krid1 WITH new_krid
        """
        Then command return code should be "0"

        When I run SQL on host "router2-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Dataspace ID":"default",
            "Lower bound":"0",
            "Shard ID":"sh1",
            "Upper bound":"deprecated"
        }]
        """

    Scenario: Move is executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE SHARDING RULE r1 COLUMN id;
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1
        """
        Then command return code should be "0"

        When I run SQL on host "router"
        """
        CREATE TABLE test(id int);
        INSERT INTO test(id) VALUES(2);
        INSERT INTO test(id) VALUES(7);
        """
        Then command return code should be "0"

        When I run SQL on host "router-admin"
        """
        MOVE KEY RANGE krid1 TO sh2
        """
        Then command return code should be "0"

        When I run SQL on host "shard2"
        """
        SELECT * FROM test
        """
        Then SQL result should match json_exactly
        """
        [{
            "id":2
        },
        {
            "id":7
        }]
        """

    Scenario: Show key_ranges/sharding_rules is executed in router
        When I run SQL on host "router-admin"
        """
        CREATE KEY RANGE old_krid FROM 0 ROUTE TO sh2;
        CREATE SHARDING RULE old_rule COLUMN id;
        UNREGISTER ROUTER r1;
        CREATE KEY RANGE new_krid FROM 100 ROUTE TO sh1;
        CREATE SHARDING RULE new_rule COLUMN xid;
        """
        Then command return code should be "0"

        When I run SQL on host "router-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"old_krid",
            "Dataspace ID":"default",
            "Lower bound":"0",
            "Shard ID":"sh2",
            "Upper bound":"deprecated"
        }]
        """

        When I run SQL on host "router-admin"
        """
        SHOW sharding_rules
        """
        Then SQL result should match json_exactly
        """
        [{
            "Columns":"id",
            "Dataspace ID":"default",
            "Hash Function":"x->x",
            "Sharding Rule ID":"old_rule",
            "Table Name":"*"
        }]
        """

    Scenario: Show routers is executed in coordinator
        When I run SQL on host "router-admin"
        """
        SHOW routers
        """
        Then SQL result should match regexp
        """
        router r1-regress_router:7000
        """
        And SQL result should match regexp
        """
        router r2-regress_router_2:7000
        """