(ns konserve-jdbc.core-sqlserver-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [release]]
            [konserve.store :as store]
            [konserve-jdbc.util :refer [test-multi-operations-sync test-multi-operations-async
                                        test-conditional-writes
                                        test-concurrent-fenced-counter
                                        test-concurrent-create-if-absent
                                        test-duplicate-insert-is-classified
                                        test-comparison-is-byte-exact
                                        default-num-keys]])
  (:import [java.util UUID]))

(def db-spec
  {:backend :jdbc
   :dbtype "sqlserver"
   :dbname "tempdb"
   :host "localhost"
   :user "sa"
   :password "passwordA1!"
   :id (UUID/randomUUID)})

(deftest jdbc-compliance-sync-test
  (let [spec (assoc db-spec :table "compliance_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-compliance-async-test
  (let [spec (assoc db-spec :table "compliance_test")
        _ (<!! (store/delete-store spec {:sync? false}))
        store (<!! (store/connect-store spec {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))

(deftest jdbc-multi-operations-sync-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Multi-operations test with synchronous store"
      (test-multi-operations-sync store "SQL Server" default-num-keys))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-operations-async-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (<!! (store/delete-store spec {:sync? false}))
        store (<!! (store/connect-store spec {:sync? false}))]
    (testing "Multi-operations test with asynchronous store"
      (test-multi-operations-async store "SQL Server" default-num-keys))
    (<!! (release store {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))

;; The same server as `core-mssql-test`, reached under the other dbtype name.
;; Both spellings map to the same statements and the same domain, and this is
;; where that stays true.
(deftest jdbc-conditional-write-test
  (let [spec (assoc db-spec :table "conditional_test_ss")
        _ (store/delete-store spec {:sync? true})]
    (testing "Conditional write contract on SQL Server"
      (test-conditional-writes #(do (store/delete-store spec {:sync? true})
                                    (store/connect-store spec {:sync? true}))
                               #(release % {:sync? true})))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-fence-mechanism-test
  (let [spec (assoc db-spec :table "fence_mechanism_test_ss")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "The database refuses a duplicate key in a way we can classify (SQL Server)"
      (test-duplicate-insert-is-classified store))
    (testing "The fenced comparison is byte-exact (SQL Server)"
      (test-comparison-is-byte-exact store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-fenced-counter-test
  (let [spec (assoc db-spec :table "fenced_counter_test_ss")
        _ (store/delete-store spec {:sync? true})]
    (testing "Concurrent fenced increments on SQL Server lose nothing"
      (test-concurrent-fenced-counter #(store/connect-store spec {:sync? true})
                                      #(release % {:sync? true})
                                      4 15))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-create-if-absent-test
  (let [spec (assoc db-spec :table "contested_create_test_ss")
        _ (store/delete-store spec {:sync? true})]
    (testing "Contested create-if-absent on SQL Server has exactly one winner"
      (test-concurrent-create-if-absent #(store/connect-store spec {:sync? true})
                                        #(release % {:sync? true})
                                        4))
    (store/delete-store spec {:sync? true})))
