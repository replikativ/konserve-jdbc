(ns konserve-jdbc.core-h2-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :refer [release]]
            [konserve.store :as store]
            [konserve-jdbc.util :refer [with-dir test-multi-operations-sync
                                        test-multi-operations-async
                                        test-conditional-writes
                                        test-concurrent-fenced-counter
                                        test-concurrent-create-if-absent
                                        test-duplicate-insert-is-classified
                                        test-comparison-is-byte-exact
                                        test-enumeration-does-not-break-fenced-writes
                                        test-multi-read-cannot-poison-a-fenced-write
                                        test-only-a-fenced-write-deposits
                                        default-num-keys]])
  (:import [java.util UUID]))

(use-fixtures :once (partial with-dir "./tmp/h2"))

(def db-spec
  {:backend :jdbc
   :dbtype "h2"
   :dbname "./tmp/h2/konserve;DB_CLOSE_ON_EXIT=FALSE"
   :user "sa"
   :password ""
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
      (test-multi-operations-sync store "H2" default-num-keys))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-operations-async-test
  (let [spec (assoc db-spec :table "multi_test")
        _ (<!! (store/delete-store spec {:sync? false}))
        store (<!! (store/connect-store spec {:sync? false}))]
    (testing "Multi-operations test with asynchronous store"
      (test-multi-operations-async store "H2" default-num-keys))
    (<!! (release store {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))

(deftest jdbc-conditional-write-test
  (let [spec (assoc db-spec :table "conditional_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Conditional write contract on H2"
      (test-conditional-writes #(do (store/delete-store spec {:sync? true})
                                    (store/connect-store spec {:sync? true}))
                               #(release % {:sync? true})
                               :machine))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-fenced-counter-test
  (let [spec (assoc db-spec :table "fenced_counter_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Concurrent fenced increments on H2 lose nothing"
      (test-concurrent-fenced-counter #(store/connect-store spec {:sync? true})
                                      #(release % {:sync? true})
                                      4 15))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-fence-mechanism-test
  (let [spec (assoc db-spec :table "fence_mechanism_test")
        _ (store/delete-store spec {:sync? true})
        store (store/connect-store spec {:sync? true})]
    (testing "The database refuses a duplicate key in a way we can classify (H2)"
      (test-duplicate-insert-is-classified store))
    (testing "The fenced comparison is byte-exact (H2)"
      (test-comparison-is-byte-exact store))
    (release store {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest jdbc-concurrent-create-if-absent-test
  (let [spec (assoc db-spec :table "contested_create_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Contested create-if-absent on H2 has exactly one winner"
      (test-concurrent-create-if-absent #(store/connect-store spec {:sync? true})
                                        #(release % {:sync? true})
                                        4))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-enumeration-vs-fenced-write-test
  (let [spec (assoc db-spec :table "swept_counter_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "A k/keys sweep does not manufacture conflicts on H2"
      (test-enumeration-does-not-break-fenced-writes #(store/connect-store spec {:sync? true})
                                                     #(release % {:sync? true})
                                                     150))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-multi-read-poisoning-test
  (let [spec (assoc db-spec :table "poison_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "A multi-read cannot redirect a fenced write on H2"
      (test-multi-read-cannot-poison-a-fenced-write #(store/connect-store spec {:sync? true})
                                                    #(release % {:sync? true})))
    (store/delete-store spec {:sync? true})))

(deftest jdbc-deposit-gate-test
  (let [spec (assoc db-spec :table "deposit_gate_test")
        _ (store/delete-store spec {:sync? true})]
    (testing "Only a conditional write's own read deposits metadata on H2"
      (test-only-a-fenced-write-deposits #(store/connect-store spec {:sync? true})
                                         #(release % {:sync? true})))
    (store/delete-store spec {:sync? true})))
