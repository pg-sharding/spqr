Feature: spqrdump test
  Background:
    #
    # Make host "coordinator" take control
    #
    Given cluster is up and running
    And host "coordinator2" is stopped
    And host "coordinator2" is started

    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS regress_router::7000
    """
    Then command return code should be "0"

  Scenario: dump via GRPC works
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    """
    Then command return code should be "0"
    
    When I run command on host "router"
    """
    /spqr/spqrdump dump -e regress_router:7000
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    """

  Scenario: dump via GRPC works with multidimensional distribution key
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer, varchar;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id, id_2;
    """
    Then command return code should be "0"

    When I run command on host "router"
    """
    /spqr/spqrdump dump -e regress_router:7000
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer, varchar;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id, id_2;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    """

  Scenario: dump via GRPC works with hashed distribution key
    When I run SQL on host "coordinator"
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id HASH FUNCTION murmur;
    """
    Then command return code should be "0"

    When I run command on host "router"
    """
    /spqr/spqrdump dump -e regress_router:7000
    """
    Then command return code should be "0"
    And command output should match regexp
    """
    CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
    ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id HASH FUNCTION murmur;
    CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
    CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
    """
