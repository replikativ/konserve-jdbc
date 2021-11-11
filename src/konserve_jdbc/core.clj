(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [konserve.impl.default :refer [new-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store]]
            [konserve.compressor :refer [null-compressor]]
            [konserve.encryptor :refer [null-encryptor]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [go <!! chan close! put!]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [taoensso.timbre :refer [warn]])
  (:import [java.sql Blob]
           (java.io ByteArrayInputStream)))

(set! *warn-on-reflection* 1)

(def ^:const default-table "konserve")
(def ^:const dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver" "mssql"])
(def ^:const supported-dbtypes #{"h2" "mysql" "postgresql" "sqlite" "sqlserver" "mssql"})

(defn extract-bytes [obj dbtype]
  (when obj
    (case dbtype
      "h2" (.getBytes ^Blob obj 0 (.length ^Blob obj))
      obj)))

(defn create-statement [db-type table]
  (case db-type
    ("postgresql" "sqlite")
    [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header bytea, meta bytea, value bytea)")]
    ("mssql" "sqlserver")
    [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NULL "
          "BEGIN "
          "CREATE TABLE dbo." table " (id varchar(100) primary key, header varbinary(max), meta varbinary(max), value varbinary(max)); "
          "END;")]
    [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header longblob, meta longblob, value longblob)")]))

(defn update-statement [db-type table id header meta value]
  (case db-type
    "h2"
    [(str "MERGE INTO " table " (id, header, meta, value) VALUES (?, ?, ?, ?);")
     id header meta value]
    ("postgresql" "sqlite")                                          ;
    [(str "INSERT INTO " table " (id, header, meta, value) VALUES (?, ?, ?, ?) "
          "ON CONFLICT (id) DO UPDATE "
          "SET header = excluded.header, meta = excluded.meta, value = excluded.value;")
     id header meta value]
    ("mssql" "sqlserver")
    [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
          "USING (VALUES (?, ?, ?, ?)) AS new (id, header, meta, value) "
          "ON tgt.id = new.id "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.value = new.value "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, value) VALUES (new.id, new.header, new.meta, new.value);")
     id header meta value]
    "mysql"
    [(str "REPLACE INTO " table " (id, header, meta, value) VALUES (?, ?, ?, ?);")
     id header meta value]
    [(str "MERGE " table " AS tgt "
          "USING (VALUES (?, ?, ?, ?)) AS new (id, header, meta, value) "
          "ON tgt.id = new.id "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.value = new.value "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, value) VALUES (new.id, new.header, new.meta, new.value);")
     id header meta value]))

(defn copy-row-statement [db-type table to from]
  (case db-type
    "h2"
    [(str "MERGE INTO " table " (id, header, meta, value) "
          "SELECT '" to "', header, meta, value FROM " table "  WHERE id = '" from "';")]
    ("postgresql" "sqlite")
    [(str "INSERT INTO " table " (id, header, meta, value) "
          "SELECT '" to "', header, meta, value FROM " table "  WHERE id = '" from "' "
          "ON CONFLICT (id) DO UPDATE "
          "SET header = excluded.header, meta = excluded.meta, value = excluded.value;")]
    ("mssql" "sqlserver")
    [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
          "USING (SELECT '" to "', header, meta, value FROM " table " WHERE id = '" from "') "
          "AS new (id, header, meta, value) "
          "ON (tgt.id = new.id)"
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.value = new.value "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, value) VALUES (new.id, new.header, new.meta, new.value);")]
    "mysql"
    [(str "REPLACE INTO " table " (id, header, meta, value) "
          "SELECT '" to "', header, meta, value FROM " table  " WHERE id = '" from "';")]
    [(str "MERGE INTO " table " AS tgt "
          "USING (SELECT '" to "', header, meta, value FROM " table " WHERE id = '" from "') "
          "AS new (id, header, meta, value) "
          "ON (tgt.id = new.id)"
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.value = new.value "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, value) VALUES (new.id, new.header, new.meta, new.value);")]))

(defn delete-statement [db-type table]
  (case db-type
    ("mssql" "sqlserver")
    [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NOT NULL "
          "BEGIN DROP TABLE dbo." table "; "
          "END;")]
    [(str "DROP TABLE IF EXISTS " table)]))

(defn change-row-id [datasource table from to]
  (with-open [conn (jdbc/get-connection datasource)]
    (jdbc/execute! conn
                   ["UPDATE " table " SET id = '" to "' WHERE id = '" from "';"])))

(defn read-field [table id column & {:keys [binary? locked-cb] :or {binary? false}}]
  (with-open [conn (jdbc/get-connection (:datasource table))]
    (let [res (-> (jdbc/execute! conn
                                 [(str "SELECT id," (name column) " FROM " (:table table) " WHERE id = '" id "';")]
                                 {:builder-fn rs/as-unqualified-lower-maps})
                  first
                  column)
          db-type (-> table :db-spec :dbtype)]
      (if binary?
        (locked-cb {:input-stream (when res (ByteArrayInputStream. (extract-bytes res db-type)))
                    :size nil})
        (extract-bytes res db-type)))))

(extend-protocol PBackingLock
  Boolean
  (-release [this env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord JDBCRow [table key data]
  PBackingBlob
  (-sync [this env]
    (if (:sync? env) nil (go-try- nil)))
  (-close [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [db-type (or (:dbtype (:db-spec table)) (:subprotocol (:db-spec table)))
                       {:keys [header meta value]} @data]
                   (when (and header meta value)
                     (with-open [conn (jdbc/get-connection (:datasource table))]
                       (with-open [ps (jdbc/prepare conn (update-statement db-type (:table table) key header meta value))]
                         (jdbc/execute-one! ps))))
                   (reset! data {})))))
  (-get-lock [this env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (read-field table key :header))))
  (-read-meta [this meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (read-field table key :meta))))
  (-read-value [this meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (read-field table key :value))))
  (-read-binary [this meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (read-field table key :value :binary? true :locked-cb locked-cb))))
  (-write-header [this header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (swap! data assoc :header header))))
  (-write-meta [this meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (swap! data assoc :meta meta))))
  (-write-value [this value meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (swap! data assoc :value value))))
  (-write-binary [this meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (swap! data assoc :value blob)))))

(defrecord JDBCTable [db-spec datasource table]
  PBackingStore
  (-create-blob [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (JDBCRow. this path (atom {})))))
  (-delete [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (jdbc/execute! datasource
                                [(str "DELETE FROM " table " WHERE id = '" path "';")]))))
  (-path [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- store-key)))                       ;; TODO: remove
  (-exists [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (with-open [conn (jdbc/get-connection datasource)]
                   (let [res (jdbc/execute! conn
                                            [(str "SELECT 1 FROM " table " WHERE id = '" path "';")])]
                     (not (nil? (first res))))))))
  (-copy [this from to env]
    (let [db-type (or (:dbtype db-spec) (:subprotocol db-spec))]
      (async+sync (:sync? env) *default-sync-translation*
                  (go-try- (jdbc/execute! datasource (copy-row-statement db-type table to from))))))
  (-atomic-move [this from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (change-row-id datasource table from to))))
  (-create-store [this env]
    (let [db-type (or (:dbtype db-spec) (:subprotocol db-spec))]
      (async+sync (:sync? env) *default-sync-translation*
                  (go-try- (jdbc/execute! datasource (create-statement db-type table))))))
  (-sync-store [this env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [this env]
    (let [db-type (or (:dbtype db-spec) (:subprotocol db-spec))]
      (async+sync (:sync? env) *default-sync-translation*
                  (go-try- (jdbc/execute! datasource (delete-statement db-type table))))))
  (-keys [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (with-open [conn (jdbc/get-connection datasource)]
                   (let [res' (jdbc/execute! conn
                                             [(str "SELECT id FROM " table ";")]
                                             {:builder-fn rs/as-unqualified-lower-maps})]
                     (map :id res')))))))

(defn connect-jdbc-store [db-spec & {:keys [table opts]
                                     :or {table default-table}
                                     :as params}]
  (let [complete-opts (merge {:sync? true} opts)
        datasource (jdbc/get-datasource db-spec)
        backing (JDBCTable. db-spec datasource table)
        config (merge {:table              table
                       :opts               complete-opts
                       :config             {:sync-blob? false
                                            :in-place? true
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :compressor         null-compressor
                       :encryptor          null-encryptor
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :table :opts :config))
        db-type (or (:dbtype db-spec) (:subprotocol db-spec))]
    (when-not db-type
      (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
    (when-not (supported-dbtypes db-type)
      (warn "Unsupported database type " db-type
            " - full functionality of store is only guaranteed for following database types: "  supported-dbtypes))
    (new-default-store table backing nil nil nil config))) ;; uses async+sync macro

(defn delete-store [db-spec & {:keys [table opts] :or {table default-table}}]
  (let [complete-opts (merge {:sync? true}
                             opts)
        datasource (jdbc/get-datasource db-spec)
        backing (JDBCTable. db-spec datasource table)]

    (-delete-store backing complete-opts)))

(comment
  (def db-spec
    (let [dir "devh2"]
      (.mkdirs (File. dir))
      {:dbtype   "h2"
       :dbname   (str "./" dir "/konserve;DB_CLOSE_ON_EXIT=FALSE")
       :user     "sa"
       :password ""}))

  (def db-spec
    {:dbtype "mssql"
     :dbname "tempdb"
     :host "localhost"
     :user "sa"
     :password "passwordA1!"})

  (def db-spec
    {:dbtype "mysql"
     :dbname "konserve"
     :host "localhost"
     :user "konserve"
     :password "password"})

  (def db-spec
    {:dbtype "postgresql"
     :dbname "konserve"
     :host "localhost"
     :user "konserve"
     :password "password"})

  (def db-spec
    (let [dir "devsql"]
      (.mkdirs (File. dir))
      {:dbtype   "sqlite"
       :dbname   (str "./" dir "/konserve")}))

  (def db-spec
    {:dbtype "sqlserver"
     :dbname "tempdb"
     :host "localhost"
     :user "sa"
     :password "passwordA1!"}))

(comment

  (require '[konserve.core :as k])
  (import  '[java.io File])

  (delete-store db-spec :opts {:sync? true})

  (def store (connect-jdbc-store db-spec :opts {:sync? true}))

  (time (k/assoc-in store ["foo"] {:foo "baz"} {:sync? true}))
  (k/get-in store ["foo"] nil {:sync? true})
  (k/exists? store "foo" {:sync? true})

  (time (k/assoc-in store [:bar] 42 {:sync? true}))
  (k/update-in store [:bar] inc {:sync? true})
  (k/get-in store [:bar] nil {:sync? true})
  (k/dissoc store :bar {:sync? true})

  (k/append store :error-log {:type :horrible} {:sync? true})
  (k/log store :error-log {:sync? true})

  (k/keys store {:sync? true})

  (k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
  (k/bget store :binbar (fn [{:keys [input-stream]}]
                          (map byte (slurp input-stream)))
          {:sync? true}))

(comment

  (require '[konserve.core :as k])
  (require '[clojure.core.async :refer [<!!]])

  (<!! (delete-store db-spec :opts {:sync? false}))

  (def store (<!! (connect-jdbc-store db-spec :opts {:sync? false})))

  (time (<!! (k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? false})))
  (<!! (k/get-in store ["foo"] nil {:sync? false}))
  (<!! (k/exists? store "foo" {:sync? false}))

  (time (<!! (k/assoc-in store [:bar] 42 {:sync? false})))
  (<!! (k/update-in store [:bar] inc {:sync? false}))
  (<!! (k/get-in store [:bar] nil {:sync? false}))
  (<!! (k/dissoc store :bar {:sync? false}))

  (<!! (k/append store :error-log {:type :horrible} {:sync? false}))
  (<!! (k/log store :error-log {:sync? false}))

  (<!! (k/keys store {:sync? false}))

  (<!! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
  (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
               {:sync? false})))
