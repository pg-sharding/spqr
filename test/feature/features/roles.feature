Feature: SPQR Role system
    Background:
        #
        # Make host "coordinator" take control
        #
        Given cluster environment is
        """
        ROUTER_CONFIG=/spqr/test/feature/conf/roles.yaml
        """
        Given cluster is up and running
        And host "coordinator2" is stopped
        And host "coordinator2" is started

        When I execute SQL on host "coordinator"
        """
        REGISTER ROUTER r1 ADDRESS regress_router:7000;
        """
        Then command return code should be "0"

    Scenario: reader has no access to write and admin operations but has access to 
        When I run SQL on host "coordinator" as user "reader"
        """
        DROP DISTRIBUTION ALL CASCADE;
        """
        Then SQL error on host "coordinator" should match regexp
        """
        context deadline exceeded
        """

        When I run SQL on host "router" as user "reader"
        """
        SELECT 1;
        """
        Then SQL error on host "coordinator" should match regexp
        """
        context deadline exceeded
        """

        When I run SQL on host "router" as user "reader"
        """
        SELECT 1 /* target-session-attrs: read-only */ ;
        """
        Then SQL error on host "router" should match regexp
        """
        context deadline exceeded
        """