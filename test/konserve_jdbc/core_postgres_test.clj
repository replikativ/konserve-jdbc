(ns konserve-jdbc.core-postgres-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.impl.storage-layout :as sl]
            [konserve-jdbc.core :as jc :refer [release]]
            [konserve.store :as store]
            [konserve-jdbc.util :refer [test-multi-operations-sync
                                        test-multi-operations-async
                                        test-conditional-writes
                                        test-concurrent-fenced-counter
                                        test-concurrent-create-if-absent
                                        test-duplicate-insert-is-classified
                                        test-comparison-is-byte-exact
                                        default-num-keys]])
  (:import [java.util UUID]))

(def db-spec
  {:backend :jdbc
   :dbtype "postgresql"
   :dbname "config-test"
   :host "localhost"
   :user "alice"
   :password "foo"
   :id (UUID/randomUUID)})

(def jdbc-url
  {:backend :jdbc
   :jdbcUrl "postgresql://alice:foo@localhost/config-test"
   :id (UUID/randomUUID)})

(def jdbc-url-2
  {:backend :jdbc
   :jdbcUrl "postgresql://alice:foo@localhost/config-test?sslmode=disable"
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

(deftest jdbc-url-test
  (let [spec (assoc jdbc-url :table "compliance_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-url-test-with-options
  (let [spec (assoc jdbc-url-2 :table "compliance_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-operations-sync-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Multi-operations test with synchronous store"
      (test-multi-operations-sync store "PostgreSQL" default-num-keys))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-operations-async-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (<!! (store/delete-store spec {:sync? false}))
        store (<!! (store/connect-store spec {:sync? false}))]
    (testing "Multi-operations test with asynchronous store"
      (test-multi-operations-async store "PostgreSQL" default-num-keys))
    (<!! (release store {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))
(deftest jdbc-read-miss-safe-marker-test
  (testing "JDBC backing implements PReadMissSafe (io-operation skips the -blob-exists? SELECT probe on reads)"
    (is (satisfies? sl/PReadMissSafe (jc/->JDBCTable nil nil nil nil)))))

(deftest jdbc-conditional-write-test
  (let [spec (assoc db-spec :table "conditional_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Conditional write contract on Postgres"
      (test-conditional-writes #(do (store/delete-store spec {:sync? true})
                                    (store/connect-store spec {:sync? true}))
                               #(release % {:sync? true})))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-fenced-counter-test
  (let [spec (assoc db-spec :table "fenced_counter_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Concurrent fenced increments on Postgres lose nothing"
      (test-concurrent-fenced-counter #(store/connect-store spec {:sync? true})
                                      #(release % {:sync? true})
                                      4 15))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-fence-mechanism-test
  (let [spec (assoc db-spec :table "fence_mechanism_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "The database refuses a duplicate key in a way we can classify (Postgres)"
      (test-duplicate-insert-is-classified store))
    (testing "The fenced comparison is byte-exact (Postgres)"
      (test-comparison-is-byte-exact store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-create-if-absent-test
  (let [spec (assoc db-spec :table "contested_create_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Contested create-if-absent on Postgres has exactly one winner"
      (test-concurrent-create-if-absent #(store/connect-store spec {:sync? true})
                                        #(release % {:sync? true})
                                        4))
    (store/delete-store spec {:sync? true})))
