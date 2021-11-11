# konserve-jdbc

A [JDBC](https://github.com/clojure/java.jdbc) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

<!--- Add to your leiningen dependencies:
[[http://clojars.org/io.replikativ/konserve-jdbc]][[http://clojars.org/io.replikativ/konserve-jdbc/latest-version.svg]]]]
-->

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

(<! (k/append store :error-log {:type :horrible}))
(<! (k/log store :error-log))

(<! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
(<! (k/bget store :binbar (fn [{:keys [input-stream]}]
                            (map byte (slurp input-stream)))
            {:sync? false}))
```

## Supported Databases

Not all databases available for JDBC have been tested to work with this implementation. 
Other databases might still work, but there is no guarantee. 
If you are interested in another database, please feel free to contact us.

Fully supported so far are the following databases:

1) PostgreSQL

``` clojure
(def pg {:dbtype "postgresql"
         :dbname "konserve"
         :host "localhost"
         :user "konserve"
         :password "password"})
```

2) MySQL

``` clojure
(def mysql {:dbtype "mysql"
            :dbname "konserve"
            :host "localhost"
            :user "konserve"
            :password "password"})
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

Copyright © 2021 Judith Massa, Alexander Oloo

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).