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

    Scenario: WorkloadLogger does not interrupt queries
        Given cluster is up and running
        When I run SQL on host "router-admin"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2 FOR DISTRIBUTION ds1;
        ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
        START TRACE ALL MESSAGES;
        """ 
        Then command return code should be "0"
        When I run SQL on host "router"
        """
        CREATE TABLE xMove(w_id INT, s TEXT);
        insert into xMove(w_id, s) values(1, '001');
        insert into xMove(w_id, s) values(11, '002');
        """
        Then command return code should be "0"
        When I run SQL on host "router"
        """
        SELECT * FROM xMove;
        """
        Then command return code should be "0"
        And SQL result should match regexp
        """
        001(.|\n)*002
        """

        Scenario: WorkloadReplay replays logs
        Given cluster is up and running
        When I run SQL on host "router-admin"
        """
        CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
        ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
        ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2 FOR DISTRIBUTION ds1;
        ALTER DISTRIBUTION ds1 ATTACH RELATION xMove DISTRIBUTION KEY w_id;
        START TRACE ALL MESSAGES
        """ 
        Then command return code should be "0"
        When I run SQL on host "router"
        """
        CREATE TABLE xMove(w_id INT, s TEXT);
        insert into xMove(w_id, s) values(1, '001');
        insert into xMove(w_id, s) values(11, '002')
        """ 
        Then command return code should be "0"
        When I run SQL on host "router-admin"
        """
        STOP TRACE MESSAGES
        """ 
        Then command return code should be "0"
        And file "go/mylogs.txt" on host "router" should match regexp
        """
        CREATE
        """
        When I run SQL on host "router"
        """
        DROP TABLE xMove
        """ 
        Then command return code should be "0"
        When I run command on host "router"
        """
        /spqr/spqr-workloadreplay replay -d regress -H regress_router -l /go/mylogs.txt -p 6432 -U regress
        """
        Then command return code should be "0"
        When I run SQL on host "shard1"
        """
        SELECT * FROM xMove
        """ 
        Then command return code should be "0"
        And SQL result should match regexp
        """
        001
        """
        When I run SQL on host "shard2"
        """
        SELECT * FROM xMove
        """ 
        Then command return code should be "0"
        And SQL result should match regexp
        """
        002
        """