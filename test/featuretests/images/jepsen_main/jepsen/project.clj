(defproject jepsen.mysync "0.1.0-SNAPSHOT"
  :description "mysync tests"
  :url "https://yandex.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clojure-complete "0.2.5"]
                 [jepsen "0.2.6"]
                 [zookeeper-clj "0.9.4"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [mysql/mysql-connector-java "8.0.28"]])
