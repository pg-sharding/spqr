Feature: split-unite feature
    Background:
        #
        # Run routers with coordinators
        # Stop all coordinators
        #
        Given cluster environment is
        """
        ROUTER_CONFIG=/spqr/test/feature/conf/router_with_coordinator.yaml
        ROUTER_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator.yaml
        ROUTER_2_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator_2.yaml
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

    Scenario: Split/Unite key_range are executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        SPLIT KEY RANGE new_krid FROM krid1 BY 5;
        """
        Then command return code should be "0"

        When I run SQL on host "router2-admin"
        """
        SHOW key_ranges;
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Distribution ID":"ds1",
            "Lower bound":"0",
            "Shard ID":"sh1"
        },
        {
            "Key range ID":"new_krid",
            "Distribution ID":"ds1",
            "Lower bound":"5",
            "Shard ID":"sh1"
        }]
        """

        When I run SQL on host "router-admin"
        """
        UNITE KEY RANGE krid1 WITH new_krid;
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
            "Distribution ID":"ds1",
            "Lower bound":"0",
            "Shard ID":"sh1"
        }]
        """