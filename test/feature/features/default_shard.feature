Feature: default shards test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I execute SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
    """
    Then command return code should be "0"

  Scenario: default shard happy path
    When I execute SQL on host "coordinator"
    """
    CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ADD DEFAULT SHARD sh1;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    Then command return code should be "0"

    And SQL result should match json
    """
    [{
      "key_range_id":"ds1.DEFAULT",
      "distribution_id":"ds1",
      "lower_bound":"-9223372036854775808",
      "shard_id":"sh1",
      "locked":"false"
    }]
    """

    And SQL result should match json
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1"
    }]
    """
    When I run SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 DROP DEFAULT SHARD;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1",
      "locked":"false"
    }]
    """

    When I run SQL on host "coordinator"
    """
    ALTER DISTRIBUTION ds1 ADD DEFAULT SHARD sh1;
    """
    Then command return code should be "0"

    When I run SQL on host "coordinator"
    """
    SHOW key_ranges;
    """
    And SQL result should match json_exactly
    """
    [{
      "key_range_id":"ds1.DEFAULT",
      "distribution_id":"ds1",
      "lower_bound":"-9223372036854775808",
      "shard_id":"sh1",
      "locked":"false"
    },
    {
      "key_range_id":"kr1",
      "distribution_id":"ds1",
      "lower_bound":"0",
      "shard_id":"sh1",
      "locked":"false"
    }]
    """