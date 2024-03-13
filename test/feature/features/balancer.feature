Feature: Mover test
  Background:
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
    ADD SHARD sh1 WITH HOSTS 'postgresql://regress@spqr_shard_1:6432/regress';
    ADD SHARD sh2 WITH HOSTS 'postgresql://regress@spqr_shard_2:6432/regress';
    """
    Then command return code should be "0"

  Scenario: balancer works
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE kr2 FROM 100000 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    """
    Then command return code should be "0"
    
    When I run SQL on host "router"
    """
    CREATE TABLE xMove(w_id INT, s TEXT);
    """
    Then command return code should be "0"
    When I run SQL on host "shard1"
    """
    INSERT INTO xMove (w_id, s) SELECT generate_series(1, 100000), 'sample text value';
    """
    Then command return code should be "0"
    When I run command on host "coordinator" with timeout "60" seconds
    """
    /spqr/spqr-balancer --config /spqr/test/feature/conf/balancer.yaml > /balancer.log
    """
    Then command return code should be "0"
    When I run SQL on host "shard2"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    10
    """
    When I run SQL on host "shard1"
    """
    SELECT count(*) FROM xMove
    """
    Then command return code should be "0"
    And SQL result should match regexp
    """
    99990
    """
    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{
      "Key range ID":"kr1",
      "Distribution ID":"ds1",
      "Lower bound":"0",
      "Shard ID":"sh1"
    },
    {
      "Key range ID":"kr2",
      "Distribution ID":"ds1",
      "Lower bound":"99990",
      "Shard ID":"sh2"
    }]
    """
