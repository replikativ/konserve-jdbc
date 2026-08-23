(ns konserve-jdbc.core-mysql-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [connect-store release delete-store connect-jdbc-store]]
            [konserve.store :as store]
            [konserve.core :as k]
            [konserve-jdbc.util :refer [test-multi-operations-sync
                                        test-multi-operations-async
                                        test-conditional-writes
                                        test-concurrent-fenced-counter
                                        test-concurrent-create-if-absent
                                        test-duplicate-insert-is-classified
                                        test-comparison-is-byte-exact
                                        test-enumeration-does-not-break-fenced-writes
                                        test-multi-read-cannot-poison-a-fenced-write
                                        default-num-keys]])
  (:import [java.util UUID]))

(def db-spec
  {:backend :jdbc
   :dbtype "mysql"
   :dbname "config-test"
   :host "localhost"
   :user "alice"
   :password "foo"
   :id (UUID/randomUUID)})

(def jdbc-url
  {:backend :jdbc
   :jdbcUrl "mysql://alice:foo@localhost/config-test"
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

(deftest table-test
  (let [jdbc-url2 (assoc jdbc-url :table "convenient_table")
        spec (assoc jdbc-url :table "convenient_table")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})
        store2 (store/connect-store jdbc-url2 {:sync? true})
        spec3 (assoc jdbc-url2 :table "priority_table")
        store3 (store/connect-store spec3 {:sync? true})
        _ (k/assoc-in store [:bar] 42 {:sync? true})]
    (testing "Testing multiple stores in single db with synchronous store"
      (is (= (k/get-in store  [:bar] nil {:sync? true})
             (k/get-in store2 [:bar] nil {:sync? true})))
      (is (not= (k/get-in store2 [:bar] nil {:sync? true})
                (k/get-in store3 [:bar] nil {:sync? true}))))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest connect-jdbc-store-test
  (let [_ (delete-store jdbc-url :table "compliance_test" :opts {:sync? true})
        store  (connect-jdbc-store jdbc-url :table "compliance_test" :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store jdbc-url :opts {:sync? true})))

(deftest jdbc-multi-operations-sync-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "Multi-operations test with synchronous store"
      (test-multi-operations-sync store "MySQL" default-num-keys))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-operations-async-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (<!! (store/delete-store spec {:sync? false}))
        store (<!! (store/connect-store spec {:sync? false}))]
    (testing "Multi-operations test with asynchronous store"
      (test-multi-operations-async store "MySQL" default-num-keys))
    (<!! (release store {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))
(deftest jdbc-conditional-write-test
  (let [spec (assoc db-spec :table "conditional_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Conditional write contract on MySQL"
      (test-conditional-writes #(do (store/delete-store spec {:sync? true})
                                    (store/connect-store spec {:sync? true}))
                               #(release % {:sync? true})
                               :global))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-fenced-counter-test
  (let [spec (assoc db-spec :table "fenced_counter_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Concurrent fenced increments on MySQL lose nothing"
      (test-concurrent-fenced-counter #(store/connect-store spec {:sync? true})
                                      #(release % {:sync? true})
                                      4 15))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-fence-mechanism-test
  (let [spec (assoc db-spec :table "fence_mechanism_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "The database refuses a duplicate key in a way we can classify (MySQL)"
      (test-duplicate-insert-is-classified store))
    (testing "The fenced comparison is byte-exact (MySQL)"
      (test-comparison-is-byte-exact store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-create-if-absent-test
  (let [spec (assoc db-spec :table "contested_create_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Contested create-if-absent on MySQL has exactly one winner"
      (test-concurrent-create-if-absent #(store/connect-store spec {:sync? true})
                                        #(release % {:sync? true})
                                        4))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-enumeration-vs-fenced-write-test
  (let [spec (assoc db-spec :table "swept_counter_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "A k/keys sweep does not manufacture conflicts on MySQL"
      (test-enumeration-does-not-break-fenced-writes #(store/connect-store spec {:sync? true})
                                                     #(release % {:sync? true})
                                                     150))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-read-poisoning-test
  (let [spec (assoc db-spec :table "poison_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "A multi-read cannot redirect a fenced write on MySQL"
      (test-multi-read-cannot-poison-a-fenced-write #(store/connect-store spec {:sync? true})
                                                    #(release % {:sync? true})))
    (store/delete-store spec {:sync? true})))
