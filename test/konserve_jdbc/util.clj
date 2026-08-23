(ns konserve-jdbc.util
  (:require [clojure.java.io :as io]
            [konserve.core :as k]
            [konserve.compliance-test :as ct]
            [konserve-jdbc.core :as core]
            [next.jdbc :as jdbc]
            [clojure.core.async :refer [<!!]]
            [clojure.test :refer [is]])
  (:import  [java.io File]))

(defn delete-recursively [filename]
  (let [func (fn [func f]
               (when (.isDirectory ^File f)
                 (doseq [^File f2 (.listFiles ^File f)]
                   (func func f2)))
               (try (io/delete-file f) (catch Exception _ nil)))]
    (func func (io/file filename))))

(defn with-dir [^String dir f]
  (.mkdirs (File. dir))
  (f)
  (delete-recursively dir))

;; Test configuration
;; This works with 100k keys but it make testing really slow 
;; for the sake of our sanity we leave it at 1k
(def ^:const default-num-keys 1000)

;; Helper functions for multi-operation tests
(defn generate-keys
  "Generate a vector of keys for testing"
  ([n]
   (mapv #(keyword (str "key-" %)) (range n)))
  ([]
   (generate-keys default-num-keys)))

(defn test-multi-operations-sync
  "Test multi-assoc, multi-get, and multi-dissoc synchronously.
   Inserts N keys using multi-assoc, retrieves them with multi-get, then deletes with multi-dissoc."
  [store db-name num-keys]
  (let [test-keys (generate-keys num-keys)
        ;; Build map for multi-assoc: {:key-0 {:id :key-0 :value "value-key-0"} ...}
        key-value-map (into {} (map (fn [k] [k {:id k :value (str "value-" (name k))}]) test-keys))]

    ;; Multi-assoc all N keys in one atomic operation
    (let [start (System/currentTimeMillis)]
      (k/multi-assoc store key-value-map {:sync? true})
      (let [elapsed (- (System/currentTimeMillis) start)]
        (println (format "%s: Multi-assoc'd %d keys in %d ms (%.2f keys/sec)"
                         db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))))

    ;; Verify some keys exist
    (is (k/exists? store :key-0 {:sync? true}))
    (when (> num-keys 5000)
      (let [mid-key (keyword (str "key-" (quot num-keys 2)))]
        (is (k/exists? store mid-key {:sync? true}))))
    (is (k/exists? store (last test-keys) {:sync? true}))

    ;; Count keys after assoc
    (let [all-keys (k/keys store {:sync? true})]
      (is (= num-keys (count all-keys))))

    ;; Multi-get all keys
    (let [start (System/currentTimeMillis)
          result (k/multi-get store test-keys {:sync? true})
          elapsed (- (System/currentTimeMillis) start)]
      (println (format "%s: Multi-get'd %d keys in %d ms (%.2f keys/sec)"
                       db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))
      ;; Verify we got all keys back
      (is (= num-keys (count result)))
      ;; Verify values match what we inserted
      (is (= {:id :key-0 :value "value-key-0"} (get result :key-0)))
      (is (= {:id (last test-keys) :value (str "value-" (name (last test-keys)))} (get result (last test-keys)))))

    ;; Multi-get with some missing keys (sparse map behavior)
    (let [mixed-keys [:key-0 :nonexistent-key-1 :key-1 :nonexistent-key-2]
          result (k/multi-get store mixed-keys {:sync? true})]
      ;; Should only contain existing keys
      (is (= 2 (count result)))
      (is (contains? result :key-0))
      (is (contains? result :key-1))
      (is (not (contains? result :nonexistent-key-1)))
      (is (not (contains? result :nonexistent-key-2))))

    ;; Multi-get with all missing keys
    (let [result (k/multi-get store [:missing-1 :missing-2 :missing-3] {:sync? true})]
      (is (= {} result)))

    ;; Multi-dissoc all keys using multi-dissoc
    (let [start (System/currentTimeMillis)]
      (k/multi-dissoc store test-keys {:sync? true})
      (let [elapsed (- (System/currentTimeMillis) start)]
        (println (format "%s: Multi-dissoc'd %d keys in %d ms (%.2f keys/sec)"
                         db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))))

    ;; Verify keys are gone
    (is (not (k/exists? store :key-0 {:sync? true})))
    (when (> num-keys 5000)
      (let [mid-key (keyword (str "key-" (quot num-keys 2)))]
        (is (not (k/exists? store mid-key {:sync? true})))))
    (is (not (k/exists? store (last test-keys) {:sync? true})))

    ;; Verify store is empty
    (let [all-keys (k/keys store {:sync? true})]
      (is (zero? (count all-keys))))))

(defn test-multi-operations-async
  "Test multi-assoc, multi-get, and multi-dissoc asynchronously.
   Inserts N keys using multi-assoc, retrieves them with multi-get, then deletes with multi-dissoc."
  [store db-name num-keys]
  (let [test-keys (generate-keys num-keys)
        ;; Build map for multi-assoc: {:key-0 {:id :key-0 :value "value-key-0"} ...}
        key-value-map (into {} (map (fn [k] [k {:id k :value (str "value-" (name k))}]) test-keys))]

    ;; Multi-assoc all N keys in one atomic operation
    (let [start (System/currentTimeMillis)]
      (<!! (k/multi-assoc store key-value-map {:sync? false}))
      (let [elapsed (- (System/currentTimeMillis) start)]
        (println (format "%s (async): Multi-assoc'd %d keys in %d ms (%.2f keys/sec)"
                         db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))))

    ;; Verify some keys exist
    (is (<!! (k/exists? store :key-0 {:sync? false})))
    (when (> num-keys 5000)
      (let [mid-key (keyword (str "key-" (quot num-keys 2)))]
        (is (<!! (k/exists? store mid-key {:sync? false})))))
    (is (<!! (k/exists? store (last test-keys) {:sync? false})))

    ;; Count keys after assoc
    (let [all-keys (<!! (k/keys store {:sync? false}))]
      (is (= num-keys (count all-keys))))

    ;; Multi-get all keys
    (let [start (System/currentTimeMillis)
          result (<!! (k/multi-get store test-keys {:sync? false}))
          elapsed (- (System/currentTimeMillis) start)]
      (println (format "%s (async): Multi-get'd %d keys in %d ms (%.2f keys/sec)"
                       db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))
      ;; Verify we got all keys back
      (is (= num-keys (count result)))
      ;; Verify values match what we inserted
      (is (= {:id :key-0 :value "value-key-0"} (get result :key-0)))
      (is (= {:id (last test-keys) :value (str "value-" (name (last test-keys)))} (get result (last test-keys)))))

    ;; Multi-get with some missing keys (sparse map behavior)
    (let [mixed-keys [:key-0 :nonexistent-key-1 :key-1 :nonexistent-key-2]
          result (<!! (k/multi-get store mixed-keys {:sync? false}))]
      ;; Should only contain existing keys
      (is (= 2 (count result)))
      (is (contains? result :key-0))
      (is (contains? result :key-1))
      (is (not (contains? result :nonexistent-key-1)))
      (is (not (contains? result :nonexistent-key-2))))

    ;; Multi-get with all missing keys
    (let [result (<!! (k/multi-get store [:missing-1 :missing-2 :missing-3] {:sync? false}))]
      (is (= {} result)))

    ;; Multi-dissoc all keys using multi-dissoc
    (let [start (System/currentTimeMillis)]
      (<!! (k/multi-dissoc store test-keys {:sync? false}))
      (let [elapsed (- (System/currentTimeMillis) start)]
        (println (format "%s (async): Multi-dissoc'd %d keys in %d ms (%.2f keys/sec)"
                         db-name num-keys elapsed (/ (* num-keys 1000.0) elapsed)))))

    ;; Verify keys are gone
    (is (not (<!! (k/exists? store :key-0 {:sync? false}))))
    (when (> num-keys 5000)
      (let [mid-key (keyword (str "key-" (quot num-keys 2)))]
        (is (not (<!! (k/exists? store mid-key {:sync? false}))))))
    (is (not (<!! (k/exists? store (last test-keys) {:sync? false}))))

    ;; Verify store is empty
    (let [all-keys (<!! (k/keys store {:sync? false}))]
      (is (zero? (count all-keys))))))

;; ---------------------------------------------------------------------------
;; Conditional writes
;; ---------------------------------------------------------------------------

(defn test-conditional-writes
  "The contract, both ways round.

   Worth saying what this does NOT establish: konserve compares the revision it
   read against the caller's before it ever reaches the backend, so a backend
   with no storage-level comparison at all still passes this test
   single-threaded. It is the shape of the API that is checked here — the
   refusal, the domain, `absent` — and `test-concurrent-fenced-counter` is what
   checks that the fence has teeth.

   Each arm gets an EMPTY store, which means dropping the table in between and
   not merely reconnecting: `conditional-write-compliance-test` already covers
   sync AND async, so it writes `:cas-async` itself, and the async variant that
   follows would then find its create-if-absent key already present."
  [fresh! release!]
  (let [a (fresh!)]
    (try (ct/conditional-write-compliance-test a) (finally (release! a))))
  (let [b (fresh!)]
    (try (<!! (ct/async-conditional-write-compliance-test b)) (finally (release! b)))))

(defn test-concurrent-fenced-counter
  "Many writers, one key, no lost update.

   Each writer gets its OWN store — its own connection, and so its own lock
   registry — because konserve's `go-locked` serialises threads that share one
   store instance, and a test that shares one would pass with no fence at all.
   What is left after that is exactly the window the database closes: read the
   head, be descheduled, write it back over someone else's commit.

   The counter is asserted, not the conflict count: every increment that was
   told it succeeded must be in the total. A backend that silently overwrites
   loses some and the sum comes up short."
  [connect! release! writers per-writer]
  (let [stores (vec (repeatedly writers connect!))
        k :fenced-counter
        _ (k/assoc (first stores) k 0 {:sync? true})
        committed (atom 0)
        conflicts (atom 0)
        run (fn [store]
              (dotimes [_ per-writer]
                (loop [attempt 0]
                  (let [[v rev] (k/get store k nil {:sync? true :with-revision? true})
                        res (try
                              (k/assoc store k (inc v) {:sync? true :expected-revision rev})
                              ::ok
                              (catch Exception e
                                (if (= :konserve/revision-mismatch (:type (ex-data e)))
                                  (do (swap! conflicts inc) ::retry)
                                  (throw e))))]
                    (if (= ::ok res)
                      (swap! committed inc)
                      (do (Thread/sleep (long (rand-int (min 50 (inc attempt)))))
                          (recur (inc attempt))))))))]
    (try
      (doseq [f (mapv #(future (run %)) stores)] @f)
      (let [final (k/get (first stores) k nil {:sync? true})]
        (is (= (* writers per-writer) @committed))
        (is (= (* writers per-writer) final)
            (str "lost updates: " (- (* writers per-writer) final)
                 " of " (* writers per-writer) " committed increments are missing"))
        (println (format "    %d writers x %d increments -> %d, %d conflicts refused"
                         writers per-writer final @conflicts)))
      (finally
        ;; The pool is keyed by the spec, so these stores share one — closing it
        ;; twice is not an error worth failing the test over.
        (doseq [s stores] (try (release! s) (catch Exception _ nil)))))))

(defn- bytes-of [& xs] (byte-array (map unchecked-byte xs)))

(defn test-duplicate-insert-is-classified
  "The create-if-absent refusal must arrive as a REVISION MISMATCH.

   Not reachable from the public API on demand: konserve reads the key under its
   lock first, so a create-if-absent onto a key that plainly exists is rejected
   before any INSERT is sent. The INSERT is refused only when a competitor lands
   in the gap between that read and the write — a real race, and a rare one to
   schedule on purpose. So the classifier is checked directly here, because
   getting it wrong surfaces as a raw driver exception instead of the mismatch
   the caller is written to retry.

   sqlite is why this test exists: it reports no SQLSTATE at all, so a
   class-23 check alone passes every other database and drops this one."
  [store]
  (let [backing (:backing store)
        conn (:connection backing)
        tbl (:table backing)
        db-type (:dbtype (:db-spec backing))
        id "duplicate-insert-probe"
        stmt #(jdbc/execute-one! conn (core/fenced-insert-statement
                                       db-type tbl id (bytes-of 1) (bytes-of 2) (bytes-of 3)))]
    (try (jdbc/execute! conn [(str "DELETE FROM " tbl " WHERE id = ?") id]) (catch Exception _ nil))
    (stmt)
    (let [e (try (stmt) ::no-refusal (catch java.sql.SQLException e e))]
      (is (not= ::no-refusal e) "the primary key must refuse the second insert")
      (is (core/integrity-violation? e)
          (str "unclassified refusal on " db-type
               ": sqlstate=" (pr-str (.getSQLState ^java.sql.SQLException e))
               " code=" (.getErrorCode ^java.sql.SQLException e))))
    (jdbc/execute! conn [(str "DELETE FROM " tbl " WHERE id = ?") id])))

(defn test-comparison-is-byte-exact
  "The fenced UPDATE must not match metadata that merely STARTS with what we read.

   SQL Server's varbinary comparison treats trailing zero bytes as
   insignificant, so `0x010203` compares equal to a stored `0x0102030000` —
   measured, an UPDATE guarded that way reported one row changed. Serialized
   metadata ending in a zero byte is ordinary, and the write it would wave
   through is precisely the overwrite the fence exists to stop. The dialect
   statement adds a length test there; this checks that it did."
  [store]
  (let [backing (:backing store)
        conn (:connection backing)
        tbl (:table backing)
        db-type (:dbtype (:db-spec backing))
        id "byte-exact-probe"
        stored (bytes-of 1 2 3 0 0)
        prefix (bytes-of 1 2 3)]
    (try (jdbc/execute! conn [(str "DELETE FROM " tbl " WHERE id = ?") id]) (catch Exception _ nil))
    (jdbc/execute-one! conn (core/fenced-insert-statement db-type tbl id (bytes-of 1) stored (bytes-of 3)))
    (let [truncated (jdbc/execute-one!
                     conn (core/fenced-update-statement db-type tbl id (bytes-of 9) (bytes-of 9) (bytes-of 9) prefix))
          honest (jdbc/execute-one!
                  conn (core/fenced-update-statement db-type tbl id (bytes-of 1) (bytes-of 8) (bytes-of 3) stored))]
      (is (zero? (:next.jdbc/update-count truncated 0))
          (str db-type " matched a truncated prefix of the stored metadata"))
      (is (= 1 (:next.jdbc/update-count honest 0))
          (str db-type " refused a write against the metadata actually stored")))
    (jdbc/execute! conn [(str "DELETE FROM " tbl " WHERE id = ?") id])))

(defn test-concurrent-create-if-absent
  "One key, many creators, exactly one winner.

   The other half of the fence, and the half with no revision to compare: the
   losers are refused either by the read konserve takes under its lock or, when
   they slip past it, by the primary key. Both must arrive as a mismatch."
  [connect! release! writers]
  (let [stores (vec (repeatedly writers connect!))
        k :contested-create
        winners (atom 0)
        losers (atom 0)]
    (try
      (doseq [f (mapv (fn [s]
                        (future
                          (try (k/assoc s k {:by (str s)} {:sync? true :expected-revision k/absent})
                               (swap! winners inc)
                               (catch Exception e
                                 (if (= :konserve/revision-mismatch (:type (ex-data e)))
                                   (swap! losers inc)
                                   (throw e))))))
                      stores)]
        @f)
      (is (= 1 @winners) "exactly one create-if-absent may succeed")
      (is (= (dec writers) @losers) "every other creator must be told it lost")
      (finally
        (doseq [s stores] (try (release! s) (catch Exception _ nil)))))))
