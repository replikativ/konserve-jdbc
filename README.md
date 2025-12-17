# konserve-jdbc

A [JDBC](https://github.com/clojure/java.jdbc) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-jdbc/latest-version.svg)](http://clojars.org/io.replikativ/konserve-jdbc)

### Synchronous Execution

``` clojure
(require '[konserve-jdbc.core :refer [connect-jdbc-store]]
         '[konserve.core :as k])

(def db-spec
  {:dbtype "sqlite"
   :dbname "./tmp/sql/konserve"})
   
(def store (connect-jdbc-store db-spec :opts {:sync? true}))

(k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
(k/get-in store ["foo"] nil {:sync? true})
(k/exists? store "foo" {:sync? true})

(k/assoc-in store [:bar] 42 {:sync? true})
(k/update-in store [:bar] inc {:sync? true})
(k/get-in store [:bar] nil {:sync? true})
(k/dissoc store :bar {:sync? true})

;; Multi-key atomic operations
(k/multi-assoc store {:user1 {:name "Alice"} 
                       :user2 {:name "Bob"}} 
                {:sync? true})

(k/append store :error-log {:type :horrible} {:sync? true})
(k/log store :error-log {:sync? true})

(let [ba (byte-array (* 10 1024 1024) (byte 42))]
  (time (k/bassoc store "banana" ba {:sync? true})))

(k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
(k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
       {:sync? true})
               
```

### Asynchronous Execution

``` clojure
(ns test-db
  (require '[konserve-jdbc.core :refer [connect-jdbc-store]]
           '[clojure.core.async :refer [<!]]
           '[konserve.core :as k])

(def db-spec
  {:dbtype "sqlite"
   :dbname "./tmp/sql/konserve"})
   
(def store (<! (connect-jdbc-store db-spec :opts {:sync? false})))

(<! (k/assoc-in store ["foo" :bar] {:foo "baz"}))
(<! (k/get-in store ["foo"]))
(<! (k/exists? store "foo"))

(<! (k/assoc-in store [:bar] 42))
(<! (k/update-in store [:bar] inc))
(<! (k/get-in store [:bar]))
(<! (k/dissoc store :bar))

;; Multi-key atomic operations
(<! (k/multi-assoc store {:user1 {:name "Alice"} 
                           :user2 {:name "Bob"}}))

(<! (k/append store :error-log {:type :horrible}))
(<! (k/log store :error-log))

(<! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
(<! (k/bget store :binbar (fn [{:keys [input-stream]}]
                            (map byte (slurp input-stream)))
            {:sync? false}))
```
## Multitenancy
To enable the use of the same JDBC database for multiple stores all jdbc specs accept a table name.
The table can specified separately or passed in the `db-spec`. 

``` clojure
(def pg-cfg  {:dbtype  "postgresql"
              :jdbcUrl "postgresql://user:password@localhost/konserve"})

(def store-a (connect-jdbc-store pg-cfg :table "this_application" :opts {:sync? true}))
(def store-b (connect-jdbc-store pg-cfg :table "that_application" :opts {:sync? true}))

(def pg-cfg-a  {:dbtype  "postgresql"
                :jdbcUrl "postgresql://user:password@localhost/konserve"
                :table   "this_application"})


(def pg-cfg-b  {:dbtype  "postgresql"
                :jdbcUrl "postgresql://user:password@localhost/konserve"
                :table   "that_application"})


(def also-store-a (connect-jdbc-store pg-cfg-a :opts {:sync? true}))
(def also-store-b (connect-jdbc-store pg-cfg-b :opts {:sync? true}))
```
In terms of priority a table specified using the keyword argument takes priority, followed
by the one specified in the `db-spec`. If no table is specified `konserve` is used as the table name.

``` clojure
(def cfg-a  {:dbtype  "postgresql"
             :jdbcUrl "postgresql://user:password@localhost/konserve"})

(def cfg-b  {:dbtype  "postgresql"
             :jdbcUrl "postgresql://user:password@localhost/konserve"
             :table   "water"})

(def store-a (connect-jdbc-store cfg-a :opts {:sync? true})) ;; table name => konserve
(def store-b (connect-jdbc-store cfg-b :opts {:sync? true}))  ;; table name => water 
(def store-c (connect-jdbc-store cfg-b :table "fire" :opts {:sync? true})) ;;table name => fire
``````

## Multi-key Operations

This backend supports atomic multi-key operations (`multi-assoc`, `multi-get`, `multi-dissoc`), allowing you to read, write, or delete multiple keys in a single operation. All operations use JDBC transactions for atomicity.

``` clojure
;; Write multiple keys atomically
(k/multi-assoc store {:user1 {:name "Alice"}
                      :user2 {:name "Bob"}}
               {:sync? true})

;; Read multiple keys in one request
(k/multi-get store [:user1 :user2 :user3] {:sync? true})
;; => {:user1 {:name "Alice"}, :user2 {:name "Bob"}}
;; Note: Returns sparse map - only found keys are included

;; Delete multiple keys atomically
(k/multi-dissoc store [:user1 :user2] {:sync? true})
;; => {:user1 true, :user2 true}
;; Returns map indicating which keys existed before deletion

;; Async versions
(<! (k/multi-assoc store {:user1 {:name "Alice"}}))
(<! (k/multi-get store [:user1 :user2]))
(<! (k/multi-dissoc store [:user1 :user2]))
```

### Batch Limits

Operations exceeding batch limits are **automatically batched** within a single transaction - this is transparent to the caller.

**Write batch limits** (rows per batch, based on SQL parameter limits):

| Database | Rows per batch |
|----------|----------------|
| PostgreSQL | 2500 |
| SQL Server/MSSQL | 450 |
| SQLite/MySQL/H2 | 225 |

**Read batch limits** (keys per SELECT IN clause):

| Database | Keys per batch |
|----------|----------------|
| PostgreSQL | 5000 |
| H2 | 2000 |
| SQL Server/MSSQL | 1500 |
| MySQL | 1000 |
| SQLite | 500 |

**Delete batch limits** (keys per DELETE IN clause):

| Database | Keys per batch |
|----------|----------------|
| PostgreSQL | 10000 |
| SQL Server/MSSQL | 1800 |
| SQLite/MySQL/H2 | 900 |

### Implementation Details

- All operations are wrapped in JDBC transactions for atomicity (all-or-nothing)
- Uses bulk INSERT/UPSERT statements for efficient writes
- Connection pooling via c3p0 is used by default for concurrent access
- Database-specific SQL syntax is handled automatically (MERGE, ON CONFLICT, REPLACE, etc.)

## Supported Databases

**BREAKING CHANGE**: konserve-jdbc versions after `0.1.79` no longer include
actual JDBC drivers. Before you upgrade please make sure your application
provides the necessary dependencies.

Not all databases available for JDBC have been tested to work with this implementation.
Other databases might still work, but there is no guarantee. Please see working
drivers in the dev-alias in the `deps.edn` file.
If you are interested in another database, please feel free to contact us.

Fully supported so far are the following databases:

1) PostgreSQL

``` clojure
(def pg-cfg  {:dbtype "postgresql"
              :dbname "konserve"
              :host "localhost"
              :user "user"
              :password "password"})

(def pg-url  {:dbtype  "postgresql"
              :jdbcUrl "postgresql://user:password@localhost/konserve"})
```

2) MySQL

``` clojure
(def mysql-cfg {:dbtype "mysql"
                :dbname "konserve"
                :host "localhost"
                :user "user"
                :password "password"})

(def mysql-url {:dbtype  "mysql"
                :jdbcUrl "mysql://user:password@localhost/konserve"})
```

3) SQlite

``` clojure
(def sqlite {:dbtype "sqlite"
             :dbname "/konserve"})
```

4) SQLserver

``` clojure
(def sqlserver {:dbtype "sqlserver"
                :dbname "konserve"
                :host "localhost"
                :user "sa"
                :password "password"})
```

5) MSSQL

``` clojure
(def mssql {:dbtype "mssql"
            :dbname "konserve"
            :host "localhost"
            :user "sa"
            :password "password"})
```

6) H2

``` clojure
(def h2 {:dbtype "h2"
         :dbname "tmp/konserve;DB_CLOSE_ON_EXIT=FALSE"
         :user "sa"
         :password ""})
```

## Commercial support

We are happy to provide commercial support with
[lambdaforge](https://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2021-2022 Judith Massa, Alexander Oloo

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
