(ns konserve-jdbc.core-mysql-test
  (:require [clojure.test :refer [deftest testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-store release delete-store]]))

(def db-spec
  {:dbtype "mysql"
   :dbname "config-test"
   :host "localhost"
   :user "alice"
   :password "foo"})

(def db-url
  {:jdbcUrl "mysql://alice:foo@localhost/config-test"})

(deftest jdbc-compliance-sync-test
  (let [_ (delete-store db-spec :table "compliance_test" :opts {:sync? true})
        store  (connect-store db-spec :table "compliance_test" :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store db-spec :opts {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [_ (<!! (delete-store db-url :table "compliance_test" :opts {:sync? false}))
        store (<!! (connect-store db-url :table "compliance_test" :opts {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (delete-store db-url :opts {:sync? false}))))
