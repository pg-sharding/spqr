Feature: There are no leftovers in ETCD after all DROPs
    Background:
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

    Scenario: DROP TASK GROUP
        When I run SQL on host "coordinator"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        """
        Then command return code should be "0"

        Given I remember current etcd state
        When I run SQL on host "coordinator"
        """
        ATTACH CONTROL POINT after_rename_key_range_cp PANIC;
        REDISTRIBUTE KEY RANGE kr1 TO sh2 TASK GROUP tg1;
        """
        Then command return code should be "1"
        
        # Wait for a lease to expire
        And we wait for "35" seconds
        And I wait for coordinator "regress_coordinator_2" to take control   

        When I record in qdb status of move task group "tg1"
        """
        {
            "state": "ERROR",
            "msg":   "some error"
        }
        """
        And I run SQL on host "coordinator2"
        """
        DROP REDISTRIBUTE TASK tg1 CASCADE;
        DROP TASK GROUP tg1 CASCADE;
        """
        Then command return code should be "0"

        And etcd should equal remembered state ignoring prefixes
        """
        /keyranges/
        /key_range_meta/
        """

    Scenario: DROP KEY RANGE
        When I run SQL on host "coordinator"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        """
        Then command return code should be "0"

        Given I remember current etcd state
        When I run SQL on host "coordinator"
        """
        CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        LOCK KEY RANGE kr1;
        """
        Then command return code should be "0"

        And I run SQL on host "coordinator"
        """
        DROP KEY RANGE kr1;
        """
        Then command return code should be "0"

        And etcd should equal remembered state ignoring prefixes
        """
        """
