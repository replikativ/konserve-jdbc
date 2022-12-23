(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
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
           (java.io ByteArrayInputStream)
           (java.sql Connection)))

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
    [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header bytea, meta bytea, val bytea)")]
    ("mssql" "sqlserver")
    [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NULL "
          "BEGIN "
          "CREATE TABLE dbo." table " (id varchar(100) primary key, header varbinary(max), meta varbinary(max), val varbinary(max)); "
          "END;")]
    [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header longblob, meta longblob, val longblob);")]))

(defn update-statement [db-type table id header meta value]
  (case db-type
    "h2"
    [(str "MERGE INTO " table " (id, header, meta, val) VALUES (?, ?, ?, ?);")
     id header meta value]
    ("postgresql" "sqlite")                                          ;
    [(str "INSERT INTO " table " (id, header, meta, val) VALUES (?, ?, ?, ?) "
          "ON CONFLICT (id) DO UPDATE "
          "SET header = excluded.header, meta = excluded.meta, val = excluded.val;")
     id header meta value]
    ("mssql" "sqlserver")
    [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
          "USING (VALUES (?, ?, ?, ?)) AS new (id, header, meta, val) "
          "ON tgt.id = new.id "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")
     id header meta value]
    "mysql"
    [(str "REPLACE INTO " table " (id, header, meta, val) VALUES (?, ?, ?, ?);")
     id header meta value]
    [(str "MERGE " table " AS tgt "
          "USING (VALUES (?, ?, ?, ?)) AS new (id, header, meta, val) "
          "ON tgt.id = new.id "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")
     id header meta value]))

(defn copy-row-statement [db-type table to from]
  (case db-type
    "h2"
    [(str "MERGE INTO " table " (id, header, meta, val) "
          "SELECT '" to "', header, meta, val FROM " table "  WHERE id = '" from "';")]
    ("postgresql" "sqlite")
    [(str "INSERT INTO " table " (id, header, meta, val) "
          "SELECT '" to "', header, meta, val FROM " table "  WHERE id = '" from "' "
          "ON CONFLICT (id) DO UPDATE "
          "SET header = excluded.header, meta = excluded.meta, val = excluded.val;")]
    ("mssql" "sqlserver")
    [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
          "USING (SELECT '" to "', header, meta, val FROM " table " WHERE id = '" from "') "
          "AS new (id, header, meta, val) "
          "ON (tgt.id = new.id)"
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")]
    "mysql"
    [(str "REPLACE INTO " table " (id, header, meta, val) "
          "SELECT '" to "', header, meta, val FROM " table  " WHERE id = '" from "';")]
    [(str "MERGE INTO " table " AS tgt "
          "USING (SELECT '" to "', header, meta, val FROM " table " WHERE id = '" from "') "
          "AS new (id, header, meta, val) "
          "ON (tgt.id = new.id)"
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")]))

(defn delete-statement [db-type table]
  (case db-type
    ("mssql" "sqlserver")
    [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NOT NULL "
          "BEGIN DROP TABLE dbo." table "; "
          "END;")]
    [(str "DROP TABLE IF EXISTS " table)]))

(defn change-row-id [connection table from to]
  (jdbc/execute! connection
                 ["UPDATE " table " SET id = '" to "' WHERE id = '" from "';"]))

(defn read-field [db-type connection table id column & {:keys [binary? locked-cb] :or {binary? false}}]
  (let [res (-> (jdbc/execute! connection
                               [(str "SELECT id," (name column) " FROM " table " WHERE id = '" id "';")]
                               {:builder-fn rs/as-unqualified-lower-maps})
                first
                column)]
    (if binary?
      (locked-cb {:input-stream (when res (ByteArrayInputStream. (extract-bytes res db-type)))
                  :size nil})
      (extract-bytes res db-type))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord JDBCRow [table key data]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [header meta value]} @data]
                           (if (and header meta value)
                             (with-open [ps (jdbc/prepare (:connection table)
                                                          (update-statement (:dbtype (:db-spec table))
                                                                            (:table table)
                                                                            key
                                                                            header meta value))]
                               (jdbc/execute-one! ps))
                             (throw (ex-info "Updating a row is only possible if header, meta and value are set." {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (read-field (:dbtype (:db-spec table)) (:connection table) (:table table) key :header))))
  (-read-meta [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (read-field (:dbtype (:db-spec table)) (:connection table) (:table table) key :meta))))
  (-read-value [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (read-field (:dbtype (:db-spec table)) (:connection table) (:table table) key :val))))
  (-read-binary [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (read-field (:dbtype (:db-spec table)) (:connection table) (:table table) key :val
                                     :binary? true :locked-cb locked-cb))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(defrecord JDBCTable [db-spec ^Connection connection table]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (JDBCRow. this store-key (atom {})))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection
                                        [(str "DELETE FROM " table " WHERE id = '" store-key "';")]))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [res (jdbc/execute! connection
                                                  [(str "SELECT 1 FROM " table " WHERE id = '" store-key "';")])]
                           (not (nil? (first res)))))))
  (-copy [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection (copy-row-statement (:dbtype db-spec) table to from)))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (change-row-id connection table from to))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection (create-statement (:dbtype db-spec) table)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection (delete-statement (:dbtype db-spec) table))
                         (.close ^Connection connection))))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [res' (jdbc/execute! connection
                                                   [(str "SELECT id FROM " table ";")]
                                                   {:builder-fn rs/as-unqualified-lower-maps})]
                           (map :id res'))))))

(defn connect-store [db-spec & {:keys [table opts]
                                :or {table default-table}
                                :as params}]
  (let [complete-opts (merge {:sync? true} opts)
        connection (jdbc/get-connection db-spec)
        db-spec (if (:dbtype db-spec)
                  db-spec
                  (assoc db-spec :dbtype (:subprotocol db-spec)))
        backing (JDBCTable. db-spec connection table)
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? true
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :compressor         null-compressor
                       :encryptor          null-encryptor
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config))]
    (when-not (:dbtype db-spec)
      (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
    (when-not (supported-dbtypes (:dbtype db-spec))
      (warn "Unsupported database type " (:dbtype db-spec)
            " - full functionality of store is only guaranteed for following database types: "  supported-dbtypes))
    (connect-default-store backing config)))

(defn release
  "Must be called after work on database has finished in order to close connection"
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (.close ^Connection (:connection ^JDBCTable (:backing store))))))

(defn delete-store [db-spec & {:keys [table opts] :or {table default-table}}]
  (let [complete-opts (merge {:sync? true} opts)
        connection (jdbc/get-connection db-spec)
        backing (JDBCTable. db-spec connection table)]
    (-delete-store backing complete-opts)))

(comment
  (import  '[java.io File])

  (def db-spec
    (let [dir "devh2"]
      (.mkdirs (File. dir))
      {:dbtype "h2"
       :dbname (str "./" dir "/konserve;DB_CLOSE_ON_EXIT=FALSE")
       :user "sa"
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
      {:dbtype "sqlite"
       :dbname (str "./" dir "/konserve")}))

  (def db-spec
    {:dbtype "sqlserver"
     :dbname "tempdb"
     :host "localhost"
     :user "sa"
     :password "passwordA1!"}))

(comment

  (require '[konserve.core :as k])

  (delete-store db-spec :opts {:sync? true})

  (def store (connect-store db-spec :opts {:sync? true}))

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
          {:sync? true})

  (release store {:sync? true}))

(comment

  (require '[konserve.core :as k])
  (require '[clojure.core.async :refer [<!!]])

  (<!! (delete-store db-spec :opts {:sync? false}))

  (def store (<!! (connect-store db-spec :opts {:sync? false})))

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
               {:sync? false}))
  (<!! (release store {:sync? false})))
