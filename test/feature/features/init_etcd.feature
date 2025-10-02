Feature: Initialize router metadata from Etcd
    Scenario: Router initialize its metadata from Etcd when no coordinator alive
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
        And host "coordinator2" is stopped
        And host "router" is stopped
        And host "router2" is stopped
        When I run SQL on host "coordinator"
        """
        UNREGISTER ROUTER ALL;
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE krid1 FROM 19 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        """
        Then command return code should be "0"

        When host "coordinator" is stopped
        And host "router" is started

        When I run SQL on host "router-admin"
        """
        SHOW key_ranges
        """
        Then SQL result should match json_exactly
        """
        [{
            "Key range ID":"krid1",
            "Distribution ID":"ds1",
            "Lower bound":"19",
            "Shard ID":"sh1",
            "Locked":"false"
        }]
        """
