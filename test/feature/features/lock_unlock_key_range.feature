Feature: Lock/Unlock key_range are executed in coordinator
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
    Scenario: Lock/Unlock key_range are executed in coordinator
        When I run SQL on host "router-admin"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
        """
        Then command return code should be "0"

        When I run SQL on host "router"
        """
        CREATE TABLE test(id int);
        """
        Then command return code should be "0"

        When I run SQL on host "router-admin"
        """
        LOCK KEY RANGE krid1;
        """
        Then command return code should be "0"

        When I run SQL on host "router2"
        """
        SELECT * FROM test WHERE id=5;
        """
        Then SQL error on host "router2" should match regexp
        """
        key range .* is locked
        """

        When I run SQL on host "router-admin"
        """
        UNLOCK KEY RANGE krid1;
        """
        Then command return code should be "0"

        When I run SQL on host "router2"
        """
        SELECT * FROM test WHERE id=5;
        """
        Then command return code should be "0"