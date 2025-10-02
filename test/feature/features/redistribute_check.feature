Feature: Redistribution test
  Background:
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_three_shards.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_three_shards.yaml
    """

    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router:7000;
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: REDISTRIBUTE KEY RANGE CHECK works with correct config
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"
    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "0"

  Scenario: REDISTRIBUTE KEY RANGE CHECK detects missing extension
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    DROP extension IF EXISTS postgres_fdw;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    foreign-data wrapper \"postgres_fdw\" does not exist
    """

  Scenario: REDISTRIBUTE KEY RANGE CHECK checks password correctness
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"

    When I run command on host "shard2"
    """
    datadir=$(sudo -u postgres psql -p 6432 -c "SHOW data_directory" | grep 'var/lib')
    echo 'host all all all password' > $datadir/pg_hba.conf
    service postgresql reload
    """
    Then command return code should be "0"
    And I wait for host "shard2" to respond

    When I run SQL on host "shard2"
    """
    ALTER USER regress PASSWORD '12345679';
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
  
  Scenario: REDISTRIBUTE KEY RANGE CHECK checks for tables on the receiving shard
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(0, 999), 'sample text value';
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    relation "public.xmove" does not exist on the destination shard
    """
  
  Scenario: REDISTRIBUTE KEY RANGE CHECK checks for tables on the source shard
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    relation "public.xmove" does not exist on the source shard, possible misconfiguration of schema names
    """

  Scenario: REDISTRIBUTE KEY RANGE CHECK checks for distribution key in source relation
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"

    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(s TEXT);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    distribution key column "w_id" not found in relation "public.xmove" on source shard
    """

  Scenario: REDISTRIBUTE KEY RANGE CHECK checks for distribution key in destination relation
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(s TEXT);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    distribution key column "w_id" not found in relation "public.xmove" on destination shard
    """
  
  Scenario: REDISTRIBUTE KEY RANGE checks for non-deferrable constraints
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove2 DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT, pkey INT PRIMARY KEY);
    CREATE TABLE xMove2(w_id INT, skey INT REFERENCES xMove ( pkey ));
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT, pkey INT PRIMARY KEY);
    CREATE TABLE xMove2(w_id INT, skey INT REFERENCES xMove ( pkey ));
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    found non-deferrable constraint or constraint referencing non-distributed table on source shard: "xmove2_skey_fkey"
    """

    When I run SQL on host "shard1"
    """
    ALTER TABLE xMove2 ALTER CONSTRAINT xmove2_skey_fkey DEFERRABLE;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    found non-deferrable constraint or constraint referencing non-distributed table on destination shard: "xmove2_skey_fkey"
    """

    Scenario: REDISTRIBUTE KEY RANGE checks for constraints on non-distributed tables
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE xExt(w_id INT, pkey INT PRIMARY KEY);
    CREATE TABLE xMove(w_id INT, pkey INT REFERENCES xExt DEFERRABLE);
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE xExt(w_id INT, pkey INT PRIMARY KEY);
    CREATE TABLE xMove(w_id INT, pkey INT REFERENCES xExt DEFERRABLE);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    found non-deferrable constraint or constraint referencing non-distributed table on source shard: "xmove_pkey_fkey"
    """

    When I run SQL on host "shard1"
    """
    ALTER TABLE xMove DROP CONSTRAINT xmove_pkey_fkey
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    found non-deferrable constraint or constraint referencing non-distributed table on destination shard: "xmove_pkey_fkey"
    """
  
Scenario: REDISTRIBUTE KEY RANGE allows constraints on reference tables
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE REFERENCE TABLE ref;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE ref(id INT PRIMARY KEY);
    CREATE TABLE xMove(w_id INT, ext_id INT REFERENCES ref(id));
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE ref(id INT PRIMARY KEY);
    CREATE TABLE xMove(w_id INT, ext_id INT REFERENCES ref(id));
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "0"

Scenario: REDISTRIBUTE KEY RANGE ignores non-existent reference tables while checking for constraints
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE REFERENCE TABLE ref;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE xMove(w_id INT);
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "0"

Scenario: REDISTRIBUTE KEY RANGE fails if no replicated relation on destination shard
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE REFERENCE TABLE ref;
    """
    Then command return code should be "0"
    
    When I run SQL on host "shard1"
    """
    CREATE TABLE ref(id INT PRIMARY KEY);
    CREATE TABLE xMove(w_id INT);
    """
    Then command return code should be "0"

    When I run SQL on host "shard2"
    """
    CREATE TABLE xMove(w_id INT);
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator" with timeout "150" seconds
    """
    REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK;
    """
    Then command return code should be "1"
    And command output should match regexp
    """
    replicated relation "public.ref" exists on source shard, but not on destination shard
    """
