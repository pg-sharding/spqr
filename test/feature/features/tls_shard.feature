Feature: TLS connections to shards via coordinator
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml
    """
    Given cluster is up and running
    And host "coordinator2" is stopped

  Scenario: ALTER SHARD enables TLS
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test_tls DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test_tls (id INT PRIMARY KEY, data TEXT)
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
    INSERT INTO test_tls (id, data) VALUES (1, 'before tls');
    INSERT INTO test_tls (id, data) VALUES (15, 'before tls');
    """
    Then command return code should be "0"

    # Verify no TLS backend connections yet
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"count": 0}]
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"count": 0}]
    """

    # ALTER SHARD — pushes TLS config to router via gRPC and
    # invalidates pooled connections, no restart needed
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 OPTIONS (SSLMODE 'require');
    ALTER SHARD sh2 OPTIONS (SSLMODE 'require');
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (2, 'after tls');
    INSERT INTO test_tls (id, data) VALUES (16, 'after tls');
    """
    Then command return code should be "0"

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

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    DROP SHARD sh1 CASCADE;
    DROP SHARD sh2 CASCADE;
    CREATE SHARD sh1 OPTIONS (HOST 'spqr_shard_1:6432', HOST 'spqr_shard_1_replica:6432',  SSLMODE 'require');
    CREATE SHARD sh2 OPTIONS (HOST 'spqr_shard_2:6432', HOST 'spqr_shard_2_replica:6432', SSLMODE 'require');
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test_tls DISTRIBUTION KEY id;
    """
    Then command return code should be "0"

    When I run SQL on host "router-admin"
    """
    INVALIDATE BACKENDS;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test_tls (id INT PRIMARY KEY, data TEXT);
    INSERT INTO test_tls (id, data) VALUES (3, 'created with tls');
    INSERT INTO test_tls (id, data) VALUES (17, 'created with tls');
    """
    Then command return code should be "0"

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

  Scenario: SyncRouterMetadata propagates shard config updates on re-register
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds_sync COLUMN TYPES integer;
    CREATE KEY RANGE kr_sync2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds_sync;
    CREATE KEY RANGE kr_sync1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds_sync;
    ALTER DISTRIBUTION ds_sync ATTACH RELATION test_sync DISTRIBUTION KEY id;
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

    # Change shard config while router is unregistered
    When I run SQL on host "coordinator"
    """
    ALTER SHARD sh1 OPTIONS (SSLMODE 'require');
    ALTER SHARD sh2 OPTIONS (SSLMODE 'require');
    """
    Then command return code should be "0"

    # Re-register — SyncRouterMetadata should detect the config drift
    # and call UpdateShard on the router for both shards
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE test_sync (id INT PRIMARY KEY, data TEXT);
    INSERT INTO test_sync (id, data) VALUES (2, 'after resync');
    INSERT INTO test_sync (id, data) VALUES (15, 'after resync sh2');
    """
    Then command return code should be "0"

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
