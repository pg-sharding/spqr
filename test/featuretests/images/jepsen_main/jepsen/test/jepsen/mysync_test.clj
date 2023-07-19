(ns jepsen.mysync-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.mysync :as mysync]))

(def mysql_nodes ["mysync_mysql1_1.mysync_mysql_net"
                  "mysync_mysql2_1.mysync_mysql_net"
                  "mysync_mysql3_1.mysync_mysql_net"])

(def zk_nodes ["mysync_zookeeper1_1.mysync_mysql_net"
               "mysync_zookeeper2_1.mysync_mysql_net"
               "mysync_zookeeper3_1.mysync_mysql_net"])

(deftest mysync-test
  (is (:valid? (:results (jepsen/run! (mysync/mysync-test mysql_nodes zk_nodes))))))
