(ns konserve-jdbc.datahike-multitenant-test
  "Multi-tenancy through datahike: one H2 database, one datahike database per
   tenant, each on its own table. Datahike releases the konserve store at the
   end of `create-database` and `delete-database`; before the pool was reference
   counted, that closed the shared c3p0 pool under every other tenant."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [konserve-jdbc.core :as jc]
            [konserve-jdbc.util :refer [with-dir]]))

(def dir "./tmp/h2-datahike")

(use-fixtures :once (fn [f] (with-dir dir f)))

(def db-spec
  {:backend :jdbc
   :dbtype "h2"
   :dbname (str "./" dir "/tenants;DB_CLOSE_ON_EXIT=FALSE")
   :user "sa"
   :password ""})

(defn tenant-config [n]
  {:store (assoc db-spec
                 :table (str "tenant_" n)
                 :id (java.util.UUID/nameUUIDFromBytes (.getBytes (str "tenant-" n))))
   :keep-history? false
   :schema-flexibility :read})

(defn- tenant-pools []
  (into {}
        (filter (fn [[_ entry]] (= (:dbname (:db-spec entry)) (:dbname db-spec))))
        (jc/pool-status)))

(defn- tenant-refs [] (some-> (first (vals (tenant-pools))) :refs))

(defn- eventually
  "Datahike's `release` hands the store back through `konserve.store/release-store`
   with its default `{:sync? false}`, so our reference comes back on a core.async
   thread a moment after `d/release` returns. Poll for the expected registry
   state rather than asserting it synchronously."
  [pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) (pred)
        :else (do (Thread/sleep 20) (recur))))))

(defn- writes-and-reads? [conn n]
  (d/transact conn [{:tenant n :note (str "hello from " n)}])
  (= #{[(str "hello from " n)]}
     (d/q '[:find ?note :in $ ?t :where [?e :tenant ?t] [?e :note ?note]] @conn n)))

(deftest datahike-tenants-share-one-pool-test
  (testing "N datahike databases on one H2 file share one pool and survive each other's lifecycle"
    (let [tenants (range 5)
          cfgs (mapv tenant-config tenants)]
      (doseq [c cfgs] (d/create-database c))
      ;; create-database released its store each time; nothing should be held
      (is (eventually #(empty? (tenant-pools))) "create-database balances its own reference")

      (let [conns (mapv d/connect cfgs)]
        (is (= 1 (count (tenant-pools))) "five tenants, one pool")
        (is (= (count tenants) (tenant-refs)) "one reference per live connection")
        (doseq [[conn n] (map vector conns tenants)]
          (is (writes-and-reads? conn n) (str "tenant " n " round-trips")))

        (testing "deleting one tenant leaves the others' connections working"
          (d/release (first conns))
          (is (eventually #(= (dec (count tenants)) (tenant-refs))) "release hands back one reference")
          (d/delete-database (first cfgs))
          (is (eventually #(= (dec (count tenants)) (tenant-refs))) "delete-database balances its own")
          (doseq [[conn n] (rest (map vector conns tenants))]
            (is (writes-and-reads? conn n) (str "tenant " n " still works after a sibling was deleted"))))

        (testing "creating a new tenant while others are live leaves them working"
          (let [new-cfg (tenant-config 99)]
            (d/create-database new-cfg)
            (let [new-conn (d/connect new-cfg)]
              (is (writes-and-reads? new-conn 99))
              (is (= (count tenants) (tenant-refs)) "four survivors plus the newcomer")
              (doseq [[conn n] (rest (map vector conns tenants))]
                (is (writes-and-reads? conn n)))
              (d/release new-conn)
              (d/delete-database new-cfg)
              (is (eventually #(= (dec (count tenants)) (tenant-refs)))))))

        (testing "releasing every connection closes the pool"
          (doseq [conn (rest conns)] (d/release conn))
          (is (eventually #(empty? (tenant-pools))))))

      (doseq [c (rest cfgs)] (d/delete-database c))
      (is (eventually #(empty? (tenant-pools))) "delete-database balances its own reference"))))
