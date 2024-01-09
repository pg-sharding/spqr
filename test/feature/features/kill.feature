Feature: Coordinator should kill client
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
        """
    Then command return code should be "0"
    When I run SQL on host "coordinator"
        """
        SHOW routers
        """
    Then command return code should be "0"
    And SQL result should match regexp
        """
        r1-regress_router:7000(.|\n)*r2-regress_router_2:7000
        """
    Given I execute SQL on host "router"
        """
        SELECT pg_sleep(1)
        """
    And I execute SQL on host "router2"
        """
        SELECT pg_sleep(1)
        """

  Scenario: kill client
    When I run SQL on host "coordinator"
    """
    show clients;
    """
    Then command return code should be "0"
    And SQL result should not match regexp
    """
    clientID=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' --csv | head -2 | tail -1 | awk -F ',' '{print $1 }')
    """
    When I run command on host "coordinator"
    """
    clientID=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' --csv | head -2 | tail -1 | awk -F ',' '{print $1 }')
    out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "kill client $clientID;" | clearID)
test "$out" = "            kill client
------------------------------------
 the client ************ was killed
(1 row)"
    """
    Then command return code should be "0"
