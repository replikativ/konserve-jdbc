(ns konserve-jdbc.core-h2-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-jdbc.core :as jc :refer [release]]
            [konserve.core :as k]
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

;; ---------------------------------------------------------------------------
;; Connection pool lifecycle
;;
;; The pool is shared by every store on a database, so releasing one store must
;; not close it for the others. This is the downstream failure that motivated
;; the refcount: datahike's `create-database`/`delete-database` release their
;; store when done, which used to close the pool every other tenant was using.
;; ---------------------------------------------------------------------------

(def tenant-spec
  (assoc db-spec :dbname "./tmp/h2/tenants;DB_CLOSE_ON_EXIT=FALSE"))

(defn- tenant-pools
  "The registry entries belonging to `tenant-spec`'s database."
  []
  (into {}
        (filter (fn [[_ {:keys [db-spec]}]]
                  (= (:dbname db-spec) (:dbname tenant-spec))))
        (jc/pool-status)))

(defn- tenant-refs []
  (some-> (first (vals (tenant-pools))) :refs))

(defn- roundtrips? [store k]
  (k/assoc store k {:written k} {:sync? true})
  (= {:written k} (k/get store k nil {:sync? true})))

(deftest pool-is-refcounted-test
  (testing "two stores on one database share one pool, released independently"
    (let [a (jc/connect-store (assoc tenant-spec :table "tenant_a") :opts {:sync? true})
          b (jc/connect-store (assoc tenant-spec :table "tenant_b") :opts {:sync? true})]
      (is (= 1 (count (tenant-pools))) "one pool per database, not per table")
      (is (= 2 (tenant-refs)))
      (is (= :retained (jc/release a {:sync? true})) "a co-tenant still holds the pool")
      (is (= 1 (tenant-refs)))
      (is (roundtrips? b :b) "releasing one store leaves the other working")
      (is (= :already-released (jc/release a {:sync? true}))
          "a second release must not hand back a reference twice")
      (is (= 1 (tenant-refs)))
      (is (roundtrips? b :b-again))
      (is (= :closed (jc/release b {:sync? true})) "the last holder closes the pool")
      (is (empty? (tenant-pools)))
      (jc/delete-store (assoc tenant-spec :table "tenant_a") :opts {:sync? true})
      (jc/delete-store (assoc tenant-spec :table "tenant_b") :opts {:sync? true}))))

(deftest delete-store-leaves-co-tenants-alive-test
  (testing "deleting one tenant's store does not close the shared pool (downstream reproducer)"
    (let [a (jc/connect-store (assoc tenant-spec :table "doomed") :opts {:sync? true})
          b (jc/connect-store (assoc tenant-spec :table "survivor") :opts {:sync? true})]
      (is (roundtrips? b :before))
      (jc/release a {:sync? true})
      (jc/delete-store (assoc tenant-spec :table "doomed") :opts {:sync? true})
      (is (roundtrips? b :after) "the surviving store still works after a sibling is deleted")
      (is (= :closed (jc/release b {:sync? true})))
      (jc/delete-store (assoc tenant-spec :table "survivor") :opts {:sync? true}))))

(deftest force-release-and-recovery-test
  (testing ":force? closes a pool others still hold, and the next connect rebuilds it"
    (let [a (jc/connect-store (assoc tenant-spec :table "forced_a") :opts {:sync? true})
          _ (jc/connect-store (assoc tenant-spec :table "forced_b") :opts {:sync? true})]
      (is (= 2 (tenant-refs)))
      (is (= :closed (jc/release a {:sync? true :force? true})))
      (is (empty? (tenant-pools)))
      (let [b2 (jc/connect-store (assoc tenant-spec :table "forced_b" :validate-pool? true)
                                 :opts {:sync? true})]
        (is (roundtrips? b2 :recovered) "a fresh pool is built rather than a dead one handed out")
        (is (= :closed (jc/release b2 {:sync? true})))
        (jc/delete-store (assoc tenant-spec :table "forced_a") :opts {:sync? true})
        (jc/delete-store (assoc tenant-spec :table "forced_b") :opts {:sync? true})))))

(deftest failed-connect-releases-reference-test
  (testing "a connect that fails after taking a pool reference hands it back"
    (let [a (jc/connect-store (assoc tenant-spec :table "good") :opts {:sync? true})]
      (is (= 1 (tenant-refs)))
      (dotimes [_ 5]
        (is (thrown? Exception
                     (jc/connect-store (assoc tenant-spec :table "bad name!!") :opts {:sync? true}))))
      (is (= 1 (tenant-refs)) "failed connects must not pin the shared pool open")
      (is (= :closed (jc/release a {:sync? true})))
      (is (empty? (tenant-pools)))
      (jc/delete-store (assoc tenant-spec :table "good") :opts {:sync? true}))))

(deftest sync-and-async-share-one-pool-test
  (testing "the same database connected sync and async is one pool, not two"
    (let [a (jc/connect-store (assoc tenant-spec :table "mode_a") :opts {:sync? true})
          b (<!! (jc/connect-store (assoc tenant-spec :table "mode_b") :opts {:sync? false}))]
      (is (= 1 (count (tenant-pools))))
      (is (= 2 (tenant-refs)))
      (is (= :retained (jc/release a {:sync? true})))
      (is (= :closed (<!! (jc/release b {:sync? false}))))
      (jc/delete-store (assoc tenant-spec :table "mode_a") :opts {:sync? true})
      (jc/delete-store (assoc tenant-spec :table "mode_b") :opts {:sync? true}))))

(deftest caller-spec-addresses-the-pool-test
  (testing "release-pool! and remove-from-pool work with the spec the caller connected with"
    (let [a (jc/connect-store (assoc tenant-spec :table "addr") :opts {:sync? true})]
      (is (= 1 (tenant-refs)))
      (jc/remove-from-pool tenant-spec)
      (is (empty? (tenant-pools)) "the caller's own spec must address the pool")
      (is (roundtrips? a :forgotten-but-alive))
      (is (= :absent (jc/release a {:sync? true})))
      (jc/delete-store (assoc tenant-spec :table "addr") :opts {:sync? true}))))

(deftest stale-holder-cannot-close-rebuilt-pool-test
  (testing "after an out-of-band close and rebuild, old holders' releases are no-ops"
    (let [spec (assoc tenant-spec :validate-pool? true)
          a (jc/connect-store (assoc spec :table "stale_a") :opts {:sync? true})
          b (jc/connect-store (assoc spec :table "stale_b") :opts {:sync? true})]
      (is (= 2 (tenant-refs)))
      (is (= :closed (jc/release-pool! tenant-spec :force? true)) "simulated out-of-band close")
      (let [c (jc/connect-store (assoc spec :table "stale_c") :opts {:sync? true})]
        (is (= 1 (tenant-refs)))
        (is (= :stale (jc/release a {:sync? true})) "a's reference was against the dead pool")
        (is (= 1 (tenant-refs)))
        (is (roundtrips? c :survives-stale-release) "the rebuilt pool is still open")
        (is (= :stale (jc/release b {:sync? true})))
        (is (= :closed (jc/release c {:sync? true})))
        (is (empty? (tenant-pools))))
      (doseq [t ["stale_a" "stale_b" "stale_c"]]
        (jc/delete-store (assoc tenant-spec :table t) :opts {:sync? true})))))

(deftest hand-built-backing-releases-test
  (testing "a JDBCTable built by hand, with no lifecycle metadata, still releases"
    (let [backing (jc/->JDBCTable tenant-spec nil "hand_built" (atom {}))]
      (is (nil? (:state (meta backing))))
      (is (= :absent (jc/release {:backing backing} {:sync? true}))))))

(deftest ^:scale pool-scale-test
  (testing "1000 concurrent tenants share one deliberately small pool"
    (let [spec (assoc tenant-spec
                      :dbname "./tmp/h2/scale;DB_CLOSE_ON_EXIT=FALSE"
                      ;; a small pool with a real timeout: if the refcount ever
                      ;; regressed into a pool-per-store, this would exhaust the
                      ;; database rather than hang forever
                      :maxPoolSize 8 :minPoolSize 2 :checkoutTimeout 30000)
          tenants 1000
          stores (mapv deref
                       (doall (for [i (range tenants)]
                                (future (jc/connect-store (assoc spec :table (str "scale_" (mod i 10)))
                                                          :opts {:sync? true})))))
          pools (into {}
                      (filter (fn [[_ {:keys [db-spec]}]] (= (:dbname db-spec) (:dbname spec))))
                      (jc/pool-status))]
      (is (= tenants (count stores)))
      (is (= 1 (count pools)) "one pool for a thousand tenants")
      (is (= tenants (:refs (first (vals pools)))))
      (let [survivor (first stores)
            results (mapv deref (doall (for [s (rest stores)]
                                         (future (jc/release s {:sync? true})))))]
        (is (= {:retained (dec tenants)} (frequencies results))
            "no release but the last one closes anything")
        (is (roundtrips? survivor :survivor) "the last tenant standing still works")
        (is (= :closed (jc/release survivor {:sync? true}))))
      (doseq [t (range 10)]
        (jc/delete-store (assoc spec :table (str "scale_" t)) :opts {:sync? true})))))
