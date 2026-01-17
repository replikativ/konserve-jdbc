(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock
                                                  PMultiWriteBackingStore PMultiReadBackingStore
                                                  -delete-store]]
            [konserve.compressor :refer [null-compressor]]
            [konserve.encryptor :refer [null-encryptor]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [go <!! chan close! put!]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [next.jdbc.connection :as connection]
            [taoensso.timbre :refer [warn debug] :as timbre]
            [hasch.core :as hasch]
            [clojure.string :as str])
  (:import [java.sql Blob]
           [com.mchange.v2.c3p0 ComboPooledDataSource PooledDataSource]
           (java.io ByteArrayInputStream)
           (java.sql Connection)))

(set! *warn-on-reflection* 1)

(def ^:const default-table "konserve")
(def ^:const dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver" "mssql"])
(def ^:const supported-dbtypes #{"h2" "mysql" "postgresql" "sqlite" "sqlserver" "mssql"})

;; this is the link to the various connection pools
(defonce pool (atom nil))

;; each unique spec will have its own pool
(defn- pool-key [db-spec]
  (keyword
   (str (hasch/uuid  (select-keys db-spec [:dbtype :jdbcUrl :host :port :user :password :dbname :sync?])))))

(defn get-connection [db-spec]
  (let [id (pool-key db-spec)
        conn (get @pool id)]
    (if-not (nil? conn)
      conn
      (let [conns ^PooledDataSource (connection/->pool ComboPooledDataSource db-spec)
            shutdown (fn [] (.close ^PooledDataSource conns))]
        (swap! pool assoc id conns)
        (.close (jdbc/get-connection conns))
        (.addShutdownHook (Runtime/getRuntime)
                          (Thread. ^Runnable shutdown))
        conns))))

(defn remove-from-pool [db-spec]
  (let [id (pool-key db-spec)]
    (swap! pool dissoc id)))

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
          "SELECT ?, header, meta, val FROM " table " WHERE id = ?;")
     to from]
    ("postgresql" "sqlite")
    [(str "INSERT INTO " table " (id, header, meta, val) "
          "SELECT ?, header, meta, val FROM " table " WHERE id = ? "
          "ON CONFLICT (id) DO UPDATE "
          "SET header = excluded.header, meta = excluded.meta, val = excluded.val;")
     to from]
    ("mssql" "sqlserver")
    [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
          "USING (SELECT ?, header, meta, val FROM " table " WHERE id = ?) "
          "AS new (id, header, meta, val) "
          "ON (tgt.id = new.id) "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")
     to from]
    "mysql"
    [(str "REPLACE INTO " table " (id, header, meta, val) "
          "SELECT ?, header, meta, val FROM " table " WHERE id = ?;")
     to from]
    [(str "MERGE INTO " table " AS tgt "
          "USING (SELECT ?, header, meta, val FROM " table " WHERE id = ?) "
          "AS new (id, header, meta, val) "
          "ON (tgt.id = new.id) "
          "WHEN MATCHED THEN UPDATE "
          "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
          "WHEN NOT MATCHED THEN "
          "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")
     to from]))

(defn bulk-insert-statement [db-type table store-key-values]
  "Generate bulk INSERT/UPSERT statement for multiple key-value pairs.
   Returns a vector with [sql-string & parameters]."
  (let [;; Generate placeholders for VALUES clause: (?, ?, ?, ?), (?, ?, ?, ?), ...
        values-placeholder (str/join "," (repeat (count store-key-values) "(?, ?, ?, ?)"))
        ;; Flatten all parameters: [id1 header1 meta1 val1 id2 header2 meta2 val2 ...]
        params (mapcat (fn [[store-key {:keys [header meta value]}]]
                         [store-key header meta value])
                       store-key-values)]
    (case db-type
      "h2"
      (into [(str "MERGE INTO " table " (id, header, meta, val) VALUES " values-placeholder ";")]
            params)

      ("postgresql" "sqlite")
      (into [(str "INSERT INTO " table " (id, header, meta, val) VALUES " values-placeholder " "
                  "ON CONFLICT (id) DO UPDATE "
                  "SET header = excluded.header, meta = excluded.meta, val = excluded.val;")]
            params)

      "mysql"
      (into [(str "INSERT INTO " table " (id, header, meta, val) VALUES " values-placeholder " "
                  "ON DUPLICATE KEY UPDATE "
                  "header = VALUES(header), meta = VALUES(meta), val = VALUES(val);")]
            params)

      ("mssql" "sqlserver")
      (into [(str "MERGE dbo." table " WITH (HOLDLOCK) AS tgt "
                  "USING (VALUES " values-placeholder ") AS new (id, header, meta, val) "
                  "ON tgt.id = new.id "
                  "WHEN MATCHED THEN UPDATE "
                  "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
                  "WHEN NOT MATCHED THEN "
                  "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")]
            params)

      ;; Default case (generic MERGE)
      (into [(str "MERGE " table " AS tgt "
                  "USING (VALUES " values-placeholder ") AS new (id, header, meta, val) "
                  "ON tgt.id = new.id "
                  "WHEN MATCHED THEN UPDATE "
                  "SET tgt.header = new.header, tgt.meta = new.meta, tgt.val = new.val "
                  "WHEN NOT MATCHED THEN "
                  "INSERT (id, header, meta, val) VALUES (new.id, new.header, new.meta, new.val);")]
            params))))

(defn bulk-delete-statement [db-type table store-keys]
  (let [placeholders (str/join "," (repeat (count store-keys) "?"))]
    (case db-type
      ("mssql" "sqlserver")
      (into [(str "DELETE FROM dbo." table " WHERE id IN (" placeholders ");")]
            store-keys)
      (into [(str "DELETE FROM " table " WHERE id IN (" placeholders ");")]
            store-keys))))

(def read-batch-limits
  "Maximum keys per SELECT IN clause, by database type.
   Based on SQL parameter limits (1 param per key) and practical result set sizes."
  {"postgresql" 5000   ; 10k param limit, but cap at 5k for result set size
   "mssql"      1500   ; 1.8k param limit, leave headroom
   "sqlserver"  1500
   "sqlite"     500    ; 999 param limit in SQLite
   "mysql"      1000   ; 65k limit but cap for practical reasons
   "h2"         2000}) ; Conservative default

(defn bulk-select-statement
  "Generate SELECT statement for multiple keys.
   Returns a vector with [sql-string & parameters]."
  [db-type table store-keys]
  (let [placeholders (str/join "," (repeat (count store-keys) "?"))]
    (case db-type
      ("mssql" "sqlserver")
      (into [(str "SELECT id, header, meta, val FROM dbo." table " WHERE id IN (" placeholders ");")]
            store-keys)
      (into [(str "SELECT id, header, meta, val FROM " table " WHERE id IN (" placeholders ");")]
            store-keys))))

(defn delete-statement [db-type table]
  (case db-type
    ("mssql" "sqlserver")
    [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NOT NULL "
          "BEGIN DROP TABLE dbo." table "; "
          "END;")]
    [(str "DROP TABLE IF EXISTS " table)]))

(defn offset-query [db-type table offset]
  (case db-type
    ("mssql" "sqlserver")
    [(str "SELECT TOP (?) id FROM " table " WHERE id > ? ORDER BY id;") 25000 offset]
    [(str "SELECT id FROM " table " WHERE id > ? ORDER BY id LIMIT ?;") offset 25000]))

(defn table-exists-query [db-type table]
  (case db-type
    ("mssql" "sqlserver")
    [(str "SELECT TOP 1 1 FROM dbo." table ";")]
    [(str "SELECT 1 FROM " table " LIMIT 1;")]))

(defn change-row-id [connection table from to]
  (jdbc/execute! connection
                 [(str "UPDATE " table " SET id = ? WHERE id = ?;") to from]))

(defn read-field [db-type connection table id column & {:keys [binary? locked-cb] :or {binary? false}}]
  (let [res (-> (jdbc/execute! connection
                               [(str "SELECT id," (name column) " FROM " table " WHERE id = ?;") id]
                               {:builder-fn rs/as-unqualified-lower-maps})
                first
                column)]
    (if binary?
      (locked-cb {:input-stream (when res (ByteArrayInputStream. (extract-bytes res db-type)))
                  :size nil})
      (extract-bytes res db-type))))

(defn read-all [db-type connection table id]
  (let [res (-> (jdbc/execute! connection
                               [(str "SELECT id, header, meta, val FROM " table " WHERE id = ?;") id]
                               {:builder-fn rs/as-unqualified-lower-maps})
                first)]
    (into {} (for [[k v] res] [k (if (= k :id) v (extract-bytes v db-type))]))))

(defn read-meta [db-type connection table id]
  (let [res (-> (jdbc/execute! connection
                               [(str "SELECT id, header, meta FROM " table " WHERE id = ?;") id]
                               {:builder-fn rs/as-unqualified-lower-maps})
                first)]
    (into {} (for [[k v] res] [k (if (= k :id) v (extract-bytes v db-type))]))))

(defn read-operation [env db-type connection table id]
  (if (= :read-meta (:operation env))
    (read-meta db-type connection table id)
    (read-all db-type connection table id)))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord JDBCRow [table key data cache]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [header meta value]} @data]
                           (if (and header meta value)
                             (let [ps (update-statement (:dbtype (:db-spec table)) (:table table) key header meta value)]
                               (jdbc/execute-one! (:connection table) ps))
                             (throw (ex-info "Updating a row is only possible if header, meta and value are set." {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (:header @cache)
                   (reset! cache (read-operation env (:dbtype (:db-spec table)) (:connection table) (:table table) key)))
                 (-> @cache :header))))
  (-read-meta [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (:meta @cache)
                   (reset! cache (read-operation env (:dbtype (:db-spec table)) (:connection table) (:table table) key)))
                 (-> @cache :meta))))
  (-read-value [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (:val @cache)
                   (reset! cache (read-operation env (:dbtype (:db-spec table)) (:connection table) (:table table) key)))
                 (-> @cache :val))))
  (-read-binary [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (:val @cache)
                   (reset! cache (read-operation env (:dbtype (:db-spec table)) (:connection table) (:table table) key)))
                 (locked-cb {:input-stream (when (-> @cache :val) (ByteArrayInputStream. (-> @cache :val)))
                             :size nil}))))
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

(defrecord JDBCTable [db-spec connection table]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (JDBCRow. this store-key (atom {}) (atom nil)))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection
                                        [(str "DELETE FROM " table " WHERE id = ?;") store-key]))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [res (jdbc/execute! connection
                                                  [(str "SELECT 1 FROM " table " WHERE id = ?;") store-key])]
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
                (go-try-
                  ;; Using CREATE IF NOT EXISTS is regarded as a schema change. To allow the store to be used
                  ;; where schema changes are not allowed on production e.g. planetscale or the user does have schema permissions,
                  ;; we test for existence first. This triggers an exception if it doesn't exist which we catch. 
                  ;; Testing for existence in other ways is not worth the effort as it is specific to the db setup 
                  ;; not just the type
                 (let [res (try
                             (jdbc/execute! connection [(str "select 1 from " table " limit 1")])
                             (catch Exception _e
                               (debug (str "Table " table " does not exist. Attempting to create it."))
                               nil))]
                   (when (nil? res)
                     (jdbc/execute! connection (create-statement (:dbtype db-spec) table)))))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (jdbc/execute! connection (delete-statement (:dbtype db-spec) table))
                         (.close ^Connection connection))))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (letfn [(fetch-batch [offset]
                            ;; We need to lazily load rows as the entire result set cannot be loaded into memory
                            ;; The OFFSET/LIMIT approach degrades as the number of rows increases
                            ;; We use lexicographic ordering to sort on the id which is indexed giving faster performance
                            ;; 25000 is a arbitrary number of rows to fetch at a time
                           (lazy-seq
                            (let [rows (into []
                                             (map :id)
                                             (jdbc/plan connection (offset-query (:dbtype db-spec) table offset)))]
                              (when (seq rows)
                                (concat rows (fetch-batch (last rows)))))))]
                   (fetch-batch "")))))

  ;; Implementation for atomic multi-key writes
  PMultiWriteBackingStore
  (-multi-write-blobs [this store-key-values env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-key-values)
                   {}
                   (jdbc/with-transaction [tx connection]
                     (let [;; Use same batch size strategy as multi-delete
                           ;; SQL Server supports VALUES with up to 2100 parameters (4 params per row = 525 rows)
                           ;; But using conservative 1800 params (450 rows) to stay well under limit
                           ;; PostgreSQL: no hard limit, use 10000 params (2500 rows) for good performance
                           ;; SQLite: default 999 parameters, use 900 (225 rows) to stay under limit
                           batch-size (case (:dbtype db-spec)
                                        "postgresql" 2500  ;; 10000 params / 4 = 2500 rows
                                        ("mssql" "sqlserver") 450  ;; 1800 params / 4 = 450 rows
                                        225)  ;; 900 params / 4 = 225 rows (SQLite and default)

                           ;; Process key-value pairs in batches
                           process-batch (fn [batch-kvs]
                                           (when (seq batch-kvs)
                                             (let [;; Generate bulk insert statement
                                                   bulk-stmt (bulk-insert-statement (:dbtype db-spec) table batch-kvs)
                                                   ;; Execute bulk insert
                                                   exec-result (jdbc/execute! tx bulk-stmt)
                                                   ;; Determine success - different JDBC drivers return different values
                                                   success (if (number? (first exec-result))
                                                             (pos? (first exec-result))
                                                             true)]
                                               ;; Return results showing success for all keys in this batch
                                               (reduce (fn [acc [store-key _]]
                                                         (assoc acc store-key success))
                                                       {}
                                                       batch-kvs))))

                           ;; Process all key-value pairs in batches and merge results
                           all-results (reduce (fn [acc batch]
                                                 (merge acc (process-batch batch)))
                                               {}
                                               (partition-all batch-size store-key-values))]
                       all-results))))))

  ;; Implementation for atomic multi-key deletes
  (-multi-delete-blobs [this store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (jdbc/with-transaction [tx connection]
                     (let [;; SQL Server supports IN clause with up to 2100 parameters
                           ;; PostgreSQL, MySQL, H2, SQLite all support IN clause efficiently
                           ;; PostgreSQL: no hard limit, but practical limit around 10k-100k
                           ;; MySQL: max_allowed_packet limits total query size, user may not know or be able to change so use conservative appraoch
                           ;; SQLite: default 999 parameters (SQLITE_MAX_VARIABLE_NUMBER)
                           ;; H2: no specific limit documented
                           batch-size (case (:dbtype db-spec)
                                        "postgresql" 10000
                                        ("mssql" "sqlserver") 1800
                                        900)

                           ;; Process keys in batches to handle large deletion sets
                           process-batch (fn [batch-keys]
                                           (when (seq batch-keys)
                                             (let [;; First, check which keys exist
                                                   placeholders (str/join "," (repeat (count batch-keys) "?"))
                                                   select-sql (case (:dbtype db-spec)
                                                                ("mssql" "sqlserver")
                                                                (str "SELECT id FROM dbo." table " WHERE id IN (" placeholders ");")
                                                                (str "SELECT id FROM " table " WHERE id IN (" placeholders ");"))
                                                   existing-keys (->> (jdbc/execute! tx (into [select-sql] batch-keys)
                                                                                     {:builder-fn rs/as-unqualified-lower-maps})
                                                                      (map :id)
                                                                      (into #{}))

                                                   ;; Now perform bulk delete if there are keys to delete
                                                   _ (when (seq existing-keys)
                                                       (jdbc/execute! tx (bulk-delete-statement (:dbtype db-spec) table (vec existing-keys))))]

                                               ;; Return results showing which keys existed
                                               (reduce (fn [acc k]
                                                         (assoc acc k (contains? existing-keys k)))
                                                       {}
                                                       batch-keys))))

                           ;; Process all keys in batches and merge results
                           all-results (reduce (fn [acc batch]
                                                 (merge acc (process-batch batch)))
                                               {}
                                               (partition-all batch-size store-keys))]
                       all-results))))))

  ;; Implementation for atomic multi-key reads
  PMultiReadBackingStore
  (-multi-read-blobs [this store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (jdbc/with-transaction [tx connection]
                     (let [db-type (:dbtype db-spec)
                           ;; Get batch size for this database type
                           batch-size (get read-batch-limits db-type 1000)

                           ;; Process a batch of keys and return map of {store-key -> JDBCRow}
                           process-batch (fn [batch-keys]
                                           (when (seq batch-keys)
                                             (let [select-stmt (bulk-select-statement db-type table batch-keys)
                                                   rows (jdbc/execute! tx select-stmt
                                                                       {:builder-fn rs/as-unqualified-lower-maps})]
                                               ;; Build map of store-key -> JDBCRow with pre-populated cache
                                               (reduce (fn [acc row]
                                                         (let [store-key (:id row)
                                                               ;; Pre-populate cache with fetched data (eager loading)
                                                               cache-data {:id store-key
                                                                           :header (extract-bytes (:header row) db-type)
                                                                           :meta (extract-bytes (:meta row) db-type)
                                                                           :val (extract-bytes (:val row) db-type)}
                                                               jdbc-row (JDBCRow. this store-key (atom {}) (atom cache-data))]
                                                           (assoc acc store-key jdbc-row)))
                                                       {}
                                                       rows))))

                           ;; Process all keys in batches and merge results
                           all-results (reduce (fn [acc batch]
                                                 (merge acc (process-batch batch)))
                                               {}
                                               (partition-all batch-size store-keys))]
                       all-results)))))))

(defn- prepare-spec [db]
  ;; next.jdbc does not officially support the credentials in the format: driver://user:password@host/db
  ;; connection/uri->db-spec makes is possible but is rough around the edges
  ;; https://github.com/seancorfield/next-jdbc/issues/229
  (if-not (contains? db :jdbcUrl)
    db
    (let [old-url (:jdbcUrl db)
          spec (connection/uri->db-spec old-url) ;; set port to -1 if none is in the url
          port (:port spec)
          new-spec  (-> spec
                        (update :dbtype #(str/replace % #"postgres$" "postgresql")) ;the postgres driver does not support long blob
                        (assoc  :port (if (pos? port)
                                        port
                                        (-> connection/dbtypes
                                            (get (:dbtype spec))
                                            :port))))
          final-jdbc-url (-> new-spec connection/jdbc-url)
          final-spec (assoc db :jdbcUrl final-jdbc-url :dbtype (:dbtype new-spec))]
      final-spec)))

(defn connect-store [db-spec & {:keys [table opts]
                                :as params}]
  (let [table (or table (:table db-spec) default-table)
        db-spec (prepare-spec db-spec)]
    (when-not (:dbtype db-spec)
      (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))

    (when-not (supported-dbtypes (:dbtype db-spec))
      (warn "Unsupported database type " (:dbtype db-spec)
            " - full functionality of store is only guaranteed for following database types: "  supported-dbtypes))

    (System/setProperties
     (doto (java.util.Properties. (System/getProperties))
       (.put "com.mchange.v2.log.MLog" "com.mchange.v2.log.slf4j.Slf4jMLog"))) ;; using  Slf4j allows timbre to control logs.

    (let [complete-opts (merge {:sync? true} opts)
          db-spec (if (:dbtype db-spec)
                    db-spec
                    (assoc db-spec :dbtype (:subprotocol db-spec)))
          db-spec (assoc db-spec :sync? (:sync? complete-opts))
          ^PooledDataSource connection (get-connection db-spec)
          backing (JDBCTable. db-spec connection table)
          config (merge {:opts               complete-opts
                         :config             {:sync-blob? true
                                              :in-place? true
                                              :no-backup? true
                                              :lock-blob? true}
                         :default-serializer :FressianSerializer
                         :compressor         null-compressor
                         :encryptor          null-encryptor
                         :buffer-size        (* 1024 1024)}
                        (dissoc params :opts :config))]
      (connect-default-store backing config))))

(def connect-jdbc-store connect-store) ;; this is the new standard approach for store. Old signature remains for backwards compatability. 

(defn release
  "Must be called after work on database has finished in order to close connection"
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try-
               (.close ^PooledDataSource (:connection ^JDBCTable (:backing store)))
               (remove-from-pool (:db-spec ^JDBCTable (:backing store))))))

(defn delete-store [db-spec & {:keys [table opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        table (or table (:table db-spec) default-table)
        connection (jdbc/get-connection (prepare-spec db-spec))
        backing (JDBCTable. db-spec connection table)]
    (-delete-store backing complete-opts)))

;; =============================================================================
;; Multimethod Registration for konserve.store dispatch
;; =============================================================================

(defmethod store/-connect-store :jdbc
  [{:keys [dbtype dbname table] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (connect-store config))))

(defmethod store/-create-store :jdbc
  [{:keys [dbtype dbname table] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (connect-store config))))

(defmethod store/-store-exists? :jdbc
  [{:keys [dbtype table] :as config} opts]
  ;; Check if the table exists by attempting to query it
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [table (or table default-table)
                     db-spec (prepare-spec config)
                     connection (jdbc/get-connection db-spec)]
                 (try
                   (let [result (jdbc/execute! connection (table-exists-query dbtype table))]
                     (some? result))
                   (catch Exception _e
                     false)
                   (finally
                     (.close ^java.sql.Connection connection)))))))

(defmethod store/-delete-store :jdbc
  [{:keys [dbtype dbname table] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (delete-store config))))

(defmethod store/-release-store :jdbc
  [_config store opts]
  ;; Release respecting caller's sync mode
  (release store opts))

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

  ;; Test multi-dissoc
  (k/assoc-in store [:user1] {:name "Alice"} {:sync? true})
  (k/assoc-in store [:user2] {:name "Bob"} {:sync? true})
  (k/assoc-in store [:user3] {:name "Charlie"} {:sync? true})
  (k/keys store {:sync? true})
  (k/multi-dissoc store [:user1 :user2 :user3] {:sync? true})
  (k/keys store {:sync? true})

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

  ;; Test multi-dissoc (async)
  (<!! (k/assoc-in store [:user1] {:name "Alice"} {:sync? false}))
  (<!! (k/assoc-in store [:user2] {:name "Bob"} {:sync? false}))
  (<!! (k/assoc-in store [:user3] {:name "Charlie"} {:sync? false}))
  (<!! (k/keys store {:sync? false}))
  (<!! (k/multi-dissoc store [:user1 :user2 :user3] {:sync? false}))
  (<!! (k/keys store {:sync? false}))

  (<!! (k/append store :error-log {:type :horrible} {:sync? false}))
  (<!! (k/log store :error-log {:sync? false}))

  (<!! (k/keys store {:sync? false}))

  (<!! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
  (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
               {:sync? false}))
  (<!! (release store {:sync? false})))
