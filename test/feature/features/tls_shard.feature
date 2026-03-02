Feature: TLS connections to shards

  Scenario: Router uses TLS to connect to shards after config reload
    # Start with router config without TLS, enable SSL on shards,
    # then update config and reload router to use TLS
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_tls_reload.yaml
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

    # Verify SSL is enabled on shards
    When I run SQL on host "shard1"
    """
    SHOW ssl
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    on
    """

    When I run SQL on host "shard2"
    """
    SHOW ssl
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    on
    """

    # Update router config to add TLS settings and reload
    When I run command on host "router"
    """
    sed -i "/hosts:/a\\      tls:\\n        sslmode: require" $ROUTER_CONFIG
    ps uax | grep [s]pqr-router | grep -v /bin/sh | awk '{print $2}' | xargs kill -HUP
    """
    Then command return code should be "0"

    # Wait for router to reload
    And we wait for "3" seconds

    # Set up distribution and key ranges via router admin console
    When I run SQL on host "router-admin"
    """
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

    # Insert data through router (this creates new connections with TLS)
    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (1, 'shard1 data')
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    INSERT INTO test_tls (id, data) VALUES (15, 'shard2 data')
    """
    Then command return code should be "0"

    # Verify data was routed correctly
    When I run SQL on host "shard1"
    """
    SELECT data FROM test_tls WHERE id = 1
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard1 data
    """

    When I run SQL on host "shard2"
    """
    SELECT data FROM test_tls WHERE id = 15
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    shard2 data
    """

    # Verify TLS connections from router to shards
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    ^\[{"count":"0"}\]$
    """

    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    ^\[{"count":"0"}\]$
    """
