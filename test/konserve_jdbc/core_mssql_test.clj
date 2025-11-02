(ns konserve-jdbc.core-mssql-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-store release delete-store]]
            [konserve-jdbc.util :refer [test-multi-operations-sync test-multi-operations-async
                                        default-num-keys]]))

(def db-spec
  {:dbtype "mssql"
   :dbname "tempdb"
   :host "localhost"
   :user "sa"
   :password "passwordA1!"})

(deftest jdbc-compliance-sync-test
  (let [_ (delete-store db-spec :table "compliance_test"  :opts {:sync? true})
        store  (connect-store db-spec :table "compliance_test" :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store db-spec :opts {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [_ (<!! (delete-store db-spec :table "compliance_test"  :opts {:sync? false}))
        store (<!! (connect-store db-spec :table "compliance_test" :opts {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (delete-store db-spec :opts {:sync? false}))))

(deftest jdbc-multi-operations-sync-test
  (let [_ (delete-store db-spec :table "multi_test" :opts {:sync? true})
        store (connect-store db-spec :table "multi_test" :opts {:sync? true})]
    (testing "Multi-operations test with synchronous store"
      (test-multi-operations-sync store "MSSQL" default-num-keys))
    (release store {:sync? true})
    (delete-store db-spec :table "multi_test" :opts {:sync? true})))

(deftest jdbc-multi-operations-async-test
  (let [_ (<!! (delete-store db-spec :table "multi_test" :opts {:sync? false}))
        store (<!! (connect-store db-spec :table "multi_test" :opts {:sync? false}))]
    (testing "Multi-operations test with asynchronous store"
      (test-multi-operations-async store "MSSQL" default-num-keys))
    (<!! (release store {:sync? false}))
    (<!! (delete-store db-spec :table "multi_test" :opts {:sync? false}))))
