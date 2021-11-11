(ns konserve-jdbc.core-sql-lite-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-jdbc-store delete-store]]
            [konserve-jdbc.util :refer [with-dir]]))

(use-fixtures :once (partial with-dir "./tmp/sql"))

(def db-spec
  {:dbtype "sqlite"
   :dbname "./tmp/sql/konserve"})

(deftest jdbc-compliance-sync-test
  (let [_ (delete-store db-spec :table "compliance_test"  :opts {:sync? true})
        store  (connect-jdbc-store db-spec :table "compliance_test" :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (delete-store db-spec :opts {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [_ (<!! (delete-store db-spec :table "compliance_test"  :opts {:sync? false}))
        store (<!! (connect-jdbc-store db-spec :table "compliance_test" :opts {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (delete-store db-spec :opts {:sync? false}))))
