Feature: Check WorkloadLog working
    Scenario: WorkloadLogger saves queries to file
        Given cluster is up and running
        When I run SQL on host "router-admin"
        """
        START TRACE ALL MESSAGES
        """ 
        Then command return code should be "0"
        When I run SQL on host "router"
        """
        SELECT 1;
        SELECT 2
        """ 
        Then command return code should be "0"
        When I run SQL on host "router-admin"
        """
        STOP TRACE MESSAGES
        """ 
        Then command return code should be "0"
        And file "go/mylogs.txt" on host "router" should match regexp
        """
        SELECT 1(.|\n)*SELECT 2
        """