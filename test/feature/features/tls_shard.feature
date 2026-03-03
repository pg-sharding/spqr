Feature: TLS connections to shards via coordinator

  Scenario: ALTER SHARD enables TLS without router restart
    # Start cluster with coordinator mode (manage_shards_by_coordinator)
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

    # Register router and create metadata via coordinator (persisted to etcd)
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test_tls DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    # Create tables on shards
    When I run SQL on host "shard1"
    """
    CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    # Enable SSL on shard1
    When I run command on host "shard1"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard1" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond

    # Enable SSL on shard2
    When I run command on host "shard2"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard2" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard2" to respond

    # Insert data through router (non-TLS backend connections)
    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (1, 'before tls')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (15, 'before tls')
    """
    Then command return code should be "0"

    # Verify no TLS backend connections yet
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "count":0[^0-9]
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "count":0[^0-9]
    """

    # ALTER SHARD — pushes TLS config to router via gRPC and
    # invalidates pooled connections, no restart needed
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS SSLMODE 'require';
    ALTER SHARD sh2 TLS SSLMODE 'require';
    """
    Then command return code should be "0"

    # New connections use sslmode=require (old ones were marked stale)
    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (2, 'after tls')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (16, 'after tls')
    """
    Then command return code should be "0"

    # Verify data was routed correctly
    When I run SQL on host "shard1"
    """
    SELECT data FROM test_tls WHERE id = 2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after tls
    """

    When I run SQL on host "shard2"
    """
    SELECT data FROM test_tls WHERE id = 16
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after tls
    """

    # Verify TLS backend connections now exist
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """

  Scenario: CREATE SHARD with TLS enables encrypted connections
    # Start cluster with coordinator mode
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

    # Enable SSL on shard1
    When I run command on host "shard1"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard1" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond

    # Enable SSL on shard2
    When I run command on host "shard2"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard2" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard2" to respond

    # Drop default shards, re-create with TLS, set up metadata
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER ALL;
    """
    Then command return code should be "0"
    And host "router" is stopped

    When I run SQL on host "coordinator"
    """
    DROP SHARD sh1 CASCADE;
    DROP SHARD sh2 CASCADE;
    CREATE SHARD sh1 WITH HOSTS 'spqr_shard_1:6432','spqr_shard_1_replica:6432' TLS SSLMODE 'require';
    CREATE SHARD sh2 WITH HOSTS 'spqr_shard_2:6432','spqr_shard_2_replica:6432' TLS SSLMODE 'require';
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test_tls DISTRIBUTION KEY id;
    """
    Then command return code should be "0"
    And host "router" is started

    # Create tables on shards
    When I run SQL on host "shard1"
    """
    CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    # Insert data through router (TLS connections from the start)
    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (3, 'created with tls')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (17, 'created with tls')
    """
    Then command return code should be "0"

    # Verify data was routed correctly
    When I run SQL on host "shard1"
    """
    SELECT data FROM test_tls WHERE id = 3
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    created with tls
    """

    When I run SQL on host "shard2"
    """
    SELECT data FROM test_tls WHERE id = 17
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    created with tls
    """

    # Verify TLS backend connections exist
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """

  Scenario: ALTER SHARD option variants are accepted and propagated
    # Verify that every ALTER SHARD option (hosts, TLS fields) is parsed,
    # persisted via the coordinator, and the response is correct.
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds_alter COLUMN TYPES integer;
    CREATE KEY RANGE kr_alter2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds_alter;
    CREATE KEY RANGE kr_alter1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds_alter;
    ALTER DISTRIBUTION ds_alter ATTACH RELATION test_alter DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    # ALTER SHARD — change only sslmode (use 'disable' so connections still work)
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS SSLMODE 'disable'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh1
    """

    # ALTER SHARD — change only hosts (same hosts as default, just testing the command)
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 WITH HOSTS 'spqr_shard_1:6432','spqr_shard_1_replica:6432'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh1
    """

    # ALTER SHARD — set cert_file (metadata-only, won't affect connections with sslmode=disable)
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS CERT_FILE '/path/to/cert.pem'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh1
    """

    # ALTER SHARD — set key_file
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS KEY_FILE '/path/to/key.pem'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh1
    """

    # ALTER SHARD — set root_cert_file
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS ROOT_CERT_FILE '/path/to/ca.pem'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh1
    """

    # ALTER SHARD — combined: multiple options in one statement
    # Use sslmode=disable so routing still works on sh2
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh2 TLS SSLMODE 'disable' TLS CERT_FILE '/etc/ssl/cert.pem' TLS KEY_FILE '/etc/ssl/key.pem' TLS ROOT_CERT_FILE '/etc/ssl/ca.pem' WITH HOSTS 'spqr_shard_2:6432','spqr_shard_2_replica:6432'
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard id.*sh2
    """

    # Verify shards still exist and routing works after all the ALTERs
    When I run SQL on host "shard1"
    """
    CREATE TABLE IF NOT EXISTS test_alter (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE IF NOT EXISTS test_alter (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_alter (id, data) VALUES (1, 'after alter sh1')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_alter (id, data) VALUES (15, 'after alter sh2')
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    SELECT data FROM test_alter WHERE id = 1
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after alter sh1
    """

    When I run SQL on host "shard2"
    """
    SELECT data FROM test_alter WHERE id = 15
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after alter sh2
    """

  Scenario: Invalid sslmode is rejected
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"
    """
    Then command return code should be "0"

    # ALTER SHARD with invalid sslmode should fail
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS SSLMODE 'bogus'
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    invalid sslmode
    """

    # CREATE SHARD with invalid sslmode should fail
    When I run SQL on host "coordinator"
    """
    CREATE SHARD sh_bad WITH HOSTS 'localhost:5432' TLS SSLMODE 'not-a-mode'
    """
    Then command return code should be "1"
    And SQL error on host "coordinator" should match regexp
    """
    invalid sslmode
    """

  Scenario: SyncRouterMetadata propagates shard config updates on re-register
    # When the coordinator detects that a router's shard config is stale
    # (e.g. after UNREGISTER + REGISTER), SyncRouterMetadata should push
    # the updated config to the router. We verify this by checking that
    # new backend connections actually use TLS after re-register.
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds_sync COLUMN TYPES integer;
    CREATE KEY RANGE kr_sync2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds_sync;
    CREATE KEY RANGE kr_sync1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds_sync;
    ALTER DISTRIBUTION ds_sync ATTACH RELATION test_sync DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    CREATE TABLE IF NOT EXISTS test_sync (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE IF NOT EXISTS test_sync (id INT PRIMARY KEY, data TEXT)
    """
    Then command return code should be "0"

    # Enable SSL on both shards
    When I run command on host "shard1"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard1" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard1" to respond

    When I run command on host "shard2"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" -t | tr -d ' ')
    openssl req -new -x509 -days 1 -nodes -subj "/CN=shard2" -keyout $datadir/server.key -out $datadir/server.crt
    chmod 600 $datadir/server.key
    chown postgres:postgres $datadir/server.key $datadir/server.crt
    echo "ssl = on" >> $datadir/postgresql.conf
    echo "ssl_cert_file = 'server.crt'" >> $datadir/postgresql.conf
    echo "ssl_key_file = 'server.key'" >> $datadir/postgresql.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard2" to respond

    # Data routes correctly before any TLS changes (plain connections)
    When I run SQL on host "router"
    """
    INSERT INTO test_sync (id, data) VALUES (1, 'initial')
    """
    Then command return code should be "0"

    # Verify no TLS backend connections yet
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    "count":0[^0-9]
    """

    # Unregister the router so we can change shard config without
    # the router being notified in real-time
    When I run SQL on host "coordinator"
    """
    UNREGISTER ROUTER r1
    """
    Then command return code should be "0"

    # Change shard config while router is unregistered
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 TLS SSLMODE 'require';
    ALTER SHARD sh2 TLS SSLMODE 'require';
    """
    Then command return code should be "0"

    # Re-register — SyncRouterMetadata should detect the config drift
    # and call UpdateShard on the router for both shards
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"
    """
    Then command return code should be "0"

    # New connections should use TLS after the sync
    When I run SQL on host "router"
    """
    INSERT INTO test_sync (id, data) VALUES (2, 'after resync')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_sync (id, data) VALUES (15, 'after resync sh2')
    """
    Then command return code should be "0"

    # Verify data routed correctly
    When I run SQL on host "shard1"
    """
    SELECT data FROM test_sync WHERE id = 2
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after resync
    """

    When I run SQL on host "shard2"
    """
    SELECT data FROM test_sync WHERE id = 15
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    after resync sh2
    """

    # Verify TLS backend connections now exist — this proves
    # SyncRouterMetadata actually pushed the sslmode=require config
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    "count":0[^0-9]
    """
