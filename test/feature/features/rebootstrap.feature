Feature: Re-bootstrap router
    Scenario: Rebootstrap works
        Given cluster environment is
        """
        ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
        ROUTER_CONFIG_2=/spqr/test/feature/conf/router_cluster.yaml
        """
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

        When I run SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
        """
        Then command return code should be "0"
        When I run command on host "router"
        """
        apt update && apt install -y iptables && iptables -A INPUT -p tcp --dport 7000 -j REJECT
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE DISTRIBUTION ds1 (int);
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE REFERENCE TABLE t;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run command on host "router"
        """
        iptables -D INPUT -p tcp --dport 7000 -j REJECT && iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
        """
        Then command return code should be "0"
        And we wait for "10" seconds
        When I run SQL on host "router-admin"
        """
        ALTER SYSTEM REBOOTSTRAP;
        """
        Then command return code should be "0"
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW distributions ORDER BY distribution_id
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [
            {
                "distribution_id": "REPLICATED",
                "column_types":"",
                "default_shard":"not exists"
            },
            {
                "distribution_id": "ds1",
                "column_types":"integer",
                "default_shard":"not exists"
            }
        ]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW key_ranges
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "distribution_id": "ds1",
            "key_range_id": "kr1",
            "locked": "false",
            "lower_bound": "0",
            "shard_id": "sh1"
        }]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW reference_relations
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "table_name": "t",
            "column_sequence_mapping": "map[]",
            "schema_name": "public",
            "shards": "[sh1 sh2]",
            "schema_version": "1"
        }]
        """

    Scenario: Coordinator can re-bootstrap router automatically if metadata differs
        Given cluster environment is
        """
        COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_watch_routers_qdb.yaml
        ROUTER_CONFIG=/spqr/test/feature/conf/router_with_coordinator.yaml
        ROUTER_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator.yaml
        ROUTER_2_COORDINATOR_CONFIG=/spqr/test/feature/conf/router_coordinator_2.yaml
        """
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "router2" is stopped
        When I run SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"
        """
        Then command return code should be "0"
        When I run command on host "router"
        """
        apt update && apt install -y iptables && iptables -A INPUT -p tcp --dport 7000 -j REJECT
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE DISTRIBUTION ds1 (int);
        """
        Then command return code should be "1"
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run command on host "router"
        """
        iptables -D INPUT -p tcp --dport 7000 -j REJECT && iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
        """
        Then command return code should be "0"
        And we wait for "30" seconds
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW distributions
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "distribution_id": "ds1",
            "column_types":"integer",
            "default_shard":"not exists"
        }]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW key_ranges
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "distribution_id": "ds1",
            "key_range_id": "kr1",
            "locked": "false",
            "lower_bound": "0",
            "shard_id": "sh1"
        }]
        """
        When I run SQL on host "coordinator"
        """
        CALL __spqr__check_router_metadata_hash("r1");
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{"hash_equal": "true"}]
        """

    Scenario: Rebootstrap works via coordinator gRPC
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

        When I run SQL on host "router-admin"
        """
        ALTER SYSTEM REBOOTSTRAP;
        """
        Then command return code should be "1"
        Then SQL error on host "router-admin" should match regexp
        """
        cannot re-bootstrap router
        """
        When I run SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
        """
        Then command return code should be "0"
        When I run command on host "router"
        """
        apt update && apt install -y iptables && iptables -A INPUT -p tcp --dport 7000 -j REJECT
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE DISTRIBUTION ds1 (int);
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE REFERENCE TABLE t;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run command on host "router"
        """
        iptables -D INPUT -p tcp --dport 7000 -j REJECT && iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
        """
        Then command return code should be "0"
        And we wait for "10" seconds
        When I run SQL on host "router-admin"
        """
        ALTER SYSTEM REBOOTSTRAP;
        """
        Then command return code should be "0"
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW distributions ORDER BY distribution_id
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [
            {
                "distribution_id": "REPLICATED",
                "column_types":"",
                "default_shard":"not exists"
            },
            {
                "distribution_id": "ds1",
                "column_types":"integer",
                "default_shard":"not exists"
            }
        ]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW key_ranges
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "distribution_id": "ds1",
            "key_range_id": "kr1",
            "locked": "false",
            "lower_bound": "0",
            "shard_id": "sh1"
        }]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW reference_relations
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "table_name": "t",
            "column_sequence_mapping": "map[]",
            "schema_name": "public",
            "shards": "[sh1 sh2]",
            "schema_version": "1"
        }]
        """

    Scenario: Rebootstrap works without use_coordinator_init
        Given cluster environment is
        """
        ROUTER_CONFIG=/spqr/test/feature/conf/router_with_coordinator.yaml
        """
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

        When I run SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
        """
        Then command return code should be "0"
        When I run command on host "router"
        """
        apt update && apt install -y iptables && iptables -A INPUT -p tcp --dport 7000 -j REJECT
        """
        Then command return code should be "0"
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE DISTRIBUTION ds1 (int);
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run SQL on host "coordinator" with timeout "60" seconds
        """
        CREATE REFERENCE TABLE t;
        """
        Then command return code should be "1"
        And command output should match regexp
        """
        connection refused
        """
        When I run command on host "router"
        """
        iptables -D INPUT -p tcp --dport 7000 -j REJECT && iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
        """
        Then command return code should be "0"
        And we wait for "10" seconds
        When I run SQL on host "router-admin"
        """
        ALTER SYSTEM REBOOTSTRAP;
        """
        Then command return code should be "0"
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW distributions ORDER BY distribution_id
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [
            {
                "distribution_id": "REPLICATED",
                "column_types":"",
                "default_shard":"not exists"
            },
            {
                "distribution_id": "ds1",
                "column_types":"integer",
                "default_shard":"not exists"
            }
        ]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW key_ranges
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "distribution_id": "ds1",
            "key_range_id": "kr1",
            "locked": "false",
            "lower_bound": "0",
            "shard_id": "sh1"
        }]
        """
        When I run SQL on host "router-admin" with timeout "60" seconds
        """
        SHOW reference_relations
        """
        Then command return code should be "0"
        And SQL result should match json_exactly
        """
        [{
            "table_name": "t",
            "column_sequence_mapping": "map[]",
            "schema_name": "public",
            "shards": "[sh1 sh2]",
            "schema_version": "1"
        }]
        """
