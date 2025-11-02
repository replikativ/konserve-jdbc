(ns konserve-jdbc.util
  (:require [clojure.java.io :as io]
            [konserve.core :as k]
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
;; for the sake of our sanity we leave it at 10k
(def ^:const default-num-keys 10000)

;; Helper functions for multi-operation tests
(defn generate-keys
  "Generate a vector of keys for testing"
  ([n]
   (mapv #(keyword (str "key-" %)) (range n)))
  ([]
   (generate-keys default-num-keys)))

(defn test-multi-operations-sync
  "Test multi-assoc and multi-dissoc synchronously.
   First inserts N keys using multi-assoc, then deletes them using multi-dissoc."
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
  "Test multi-assoc and multi-dissoc asynchronously.
   First inserts N keys using multi-assoc, then deletes them using multi-dissoc."
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
