Feature: Proxy console simple
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

# TODO: check distributions in this test
    Scenario: Add key_range is executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        """
        Then command return code should be "0"