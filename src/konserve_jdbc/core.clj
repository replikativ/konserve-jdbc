(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [konserve.impl.default-store :refer [new-default-store]]
            [hasch.core :as hasch]
            [next.jdbc :as jdbc]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [konserve.storage-layout :refer [SplitLayout]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
            [java.sql Blob]))

(set! *warn-on-reflection* 1)
(def dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver" "mssql"])

(defn str-uuid 
  [key] 
  (str (hasch/uuid key)))

(defn extract-bytes [obj dbtype]
  (when obj
    (cond
      (= "h2" dbtype)
      (.getBytes ^Blob obj 0 (.length ^Blob obj))
      :else obj)))

(defrecord JDBCConnection [db-spec datasource table]
  PBackingStore
  (-create-blob [this store-key env]
    (JDBCRow. this (str-uuid store-key) nil nil nil))
  (-delete [this path env]
    (jdbc/execute! datasource
                   [(str "DELETE FROM " table " WHERE id = '" path "'")]))
  (-path [this store-key env]
    (str-uuid store-key))
  (-exists [this path env]
    (with-open [conn (jdbc/get-connection datasource)]
      (let [res (first (jdbc/execute! conn
                                      [(str "SELECT 1 FROM " table " WHERE id = '" path "'")]))]
        (not (nil? res)))))
  (-copy [this from to env]
    nil)
  (-atomic-move [this from to env]
    nil)
  (-create-store [this env]
    (let [db-type (or (:dbtype db-spec) (:subprotocol db-spec))]
      (when-not db-type
        (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
      (case db-type
        "postgresql"
        (jdbc/execute! datasource [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header bytea, meta bytea, data bytea)")])
        ("mssql" "sqlserver")
        (jdbc/execute! datasource [(str "IF OBJECT_ID(N'dbo." table "', N'U') IS NULL BEGIN CREATE TABLE dbo." table " (id varchar(100) primary key, meta bytea, meta varbinary(max), data varbinary(max)); END;")])
        (jdbc/execute! datasource [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, meta longblob, meta bytea, data longblob)")]))))
  (-sync-store [this env]
    nil)
  (-delete-store [this env]
    (jdbc/execute! datasource [(str "DROP TABLE " table)]))
  (-keys [this path env]
    (with-open [conn (jdbc/get-connection datasource)]
      (let [res' (jdbc/execute! conn
                                [(str "SELECT id,meta FROM " table)]
                                {:builder-fn rs/as-unqualified-lower-maps})
            res (doall (map #(extract-bytes (:meta %) (:dbtype db-spec)) res'))]
        res))))

(defrecord JDBCRow [connection key header meta data]
  PBackingBlob
  (-sync [this env]
    nil)
  (-close [this env]
    (with-open [conn (jdbc/get-connection (:datasource connection))]
      (with-open [ps (jdbc/prepare conn [(str "INSERT INTO " (:table connection) " (id,header,meta,data) values(?, ?, ?)")
                                         key
                                         header
                                         meta
                                         data])]
        (jdbc/execute-one! ps))))
  (-get-lock [this env]
    nil)

  (-read-header [this env]                                  ;; Did not exist before or different layout now?
    (with-open [conn (jdbc/get-connection (:datasource connection))]
      (let [res' (first (jdbc/execute! conn
                                       [(str "SELECT id,header FROM " (:table connection) " WHERE id = '" key "'")]
                                       {:builder-fn rs/as-unqualified-lower-maps}))
            res (extract-bytes (:header res') (-> conn :db-spec :dbtype))]
        res)))
  (-read-meta [this meta-size env]
    (with-open [conn (jdbc/get-connection (:datasource connection))]
      (let [res' (first (jdbc/execute! conn
                                       [(str "SELECT id,meta FROM " (:table connection) " WHERE id = '" key "'")]
                                       {:builder-fn rs/as-unqualified-lower-maps}))
            res (extract-bytes (:meta res') (-> conn :db-spec :dbtype))]
        res)))
  (-read-value [this meta-size env]
    (with-open [conn (jdbc/get-connection (:datasource connection))]
      (let [res' (first (jdbc/execute! conn
                                       [(str "SELECT id,data FROM " (:table connection) " WHERE id = '" key "'")]
                                       {:builder-fn rs/as-unqualified-lower-maps}))
            res (extract-bytes (:data res') (-> conn :db-spec :dbtype))]
        res)))
  (-read-binary [this meta-size locked-cb env]
    (-read-value this meta-size env))

  (-write-header [connection header-arr env]
    (JDBCRow. connection key header-arr meta value))
  (-write-meta [connection meta-arr env]
    (JDBCRow. connection key header meta-arr value))
  (-write-value [connection value-arr meta-size env]
    (JDBCRow. connection key header meta value-arr))
  (-write-binary [connection meta-size blob env]
    (JDBCRow. connection key header meta blob)))


(defn new-jdbc-store [db-spec & {:keys [table] :or {table "konserve"}}]
  (let [datasource (jdbc/get-datasource db-spec)
        backing (JDBCConnection. db-spec datasource table)]
    (new-default-store db-spec backing nil nil nil)))
