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
        CREATE SHARD new_shard OPTIONS (host 'spqr_shard_3:6432');
        ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
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
            "key_range_id":"krid1",
            "distribution_id":"ds1",
            "lower_bound":"19",
            "shard_id":"sh1",
            "locked":"false"
        }]
        """

        #
        # Shards are expected to be taken from router config
        #
        When I run SQL on host "router-admin"
        """
        SHOW shards;
        """
        Then SQL result should match json_exactly
        """
        [
            {
                "shard": "sh1",
                "options": "{host=spqr_shard_1:6432,host=spqr_shard_1_replica:6432}"
            },
            {
                "shard": "sh2",
                "options": "{host=spqr_shard_2:6432,host=spqr_shard_2_replica:6432}"
            }
        ]
        """

        When I run SQL on host "router-admin"
        """
        SHOW distributions;
        """
        Then SQL result should match json_exactly
        """
        [
            {
                "distribution_id": "ds1",
                "default_shard": "not exists",
                "column_types": "integer"
            }
        ]
        """

        When I run SQL on host "router-admin"
        """
        SHOW relations;
        """
        Then SQL result should match json_exactly
        """
        [
            {
                "distribution_id": "ds1",
                "distribution_key": "(\"id\", identity)",
                "relation_name": "test",
                "schema_name": "public"
            }
        ]
        """
