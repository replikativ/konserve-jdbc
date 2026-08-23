# konserve-jdbc

A [JDBC](https://github.com/seancorfield/next-jdbc) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/org.replikativ/konserve-jdbc/latest-version.svg)](http://clojars.org/org.replikativ/konserve-jdbc)

### Configuration

Supports all [JDBC-compatible databases](https://clojure.org/guides/deps_and_cli).

``` clojure
(require '[konserve-jdbc.core]  ;; Registers the :jdbc backend
         '[konserve.core :as k])

;; SQLite
(def sqlite-config
  {:backend :jdbc
   :dbtype "sqlite"
   :dbname "./tmp/konserve.db"
   :table "store"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"})

;; PostgreSQL
(def postgres-config
  {:backend :jdbc
   :dbtype "postgresql"
   :dbname "mydb"
   :host "localhost"
   :user "postgres"
   :password "password"
   :table "konserve"
   :id #uuid "550e8400-e29b-41d4-a716-446655440001"})

;; MySQL
(def mysql-config
  {:backend :jdbc
   :dbtype "mysql"
   :dbname "mydb"
   :host "localhost"
   :user "root"
   :password "password"
   :table "konserve"
   :id #uuid "550e8400-e29b-41d4-a716-446655440002"})

(def store (k/create-store sqlite-config {:sync? true}))
```

For API usage (assoc-in, get-in, delete-store, etc.), see the [konserve documentation](https://github.com/replikativ/konserve).

## Multitenancy

Multiple independent stores can be housed in the same database. Each store has:
- `:id` - The virtual/global identifier for the store (required, UUID)
- `:table` - The physical database table where data is stored (optional, defaults to "konserve")

``` clojure
(def db  {:dbtype  "postgresql"
          :dbname  "konserve"
          :host "localhost"
          :user "user"
          :password "password"})

;; Store A - uses table "app_a"
(def store-a-config
  (assoc db
    :backend :jdbc
    :table "app_a"
    :id #uuid "11111111-1111-1111-1111-111111111111"))

;; Store B - uses table "app_b"
(def store-b-config
  (assoc db
    :backend :jdbc
    :table "app_b"
    :id #uuid "22222222-2222-2222-2222-222222222222"))

;; Store C - uses default "konserve" table with different ID
(def store-c-config
  (assoc db
    :backend :jdbc
    :id #uuid "33333333-3333-3333-3333-333333333333"))
```

In terms of priority a table specified using the keyword argument takes priority, followed
by the one specified in the `db-spec`. If no table is specified `konserve` is used as the table name.

``` clojure
(def cfg-a  {:dbtype  "postgresql"
             :jdbcUrl "postgresql://user:password@localhost/konserve"
             :id #uuid "11111111-1111-1111-1111-111111111111"})

(def cfg-b  {:dbtype  "postgresql"
             :jdbcUrl "postgresql://user:password@localhost/konserve"
             :table   "water"
             :id #uuid "22222222-2222-2222-2222-222222222222"})

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

(def store-a (k/create-store store-a-config {:sync? true}))
(def store-b (k/create-store store-b-config {:sync? true}))
(def store-c (k/create-store store-c-config {:sync? true}))
```

The `:id` is the authoritative virtual store identity used for global identification. The `:table` determines the physical storage location. Multiple stores can share the same table but must have different `:id` values.

## Implementation Details

### Multi-key Operations

This backend supports atomic multi-key operations (`multi-assoc`, `multi-get`, `multi-dissoc`). All operations use JDBC transactions for atomicity.

**Automatic Batching**: Operations exceeding database-specific limits are automatically batched within a single transaction - this is transparent to the caller.

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

**Transaction Support**: All operations are wrapped in JDBC transactions for atomicity. Uses bulk INSERT/UPSERT statements for efficient writes. Connection pooling via c3p0 is used by default for concurrent access. Database-specific SQL syntax is handled automatically.

## Conditional Writes

Two processes that read the same key and then both write it will, by default,
leave whichever wrote last — the other update is gone, and neither writer is
told. Passing `:expected-revision` makes the write conditional on the key still
holding what was read:

``` clojure
(require '[konserve.core :as k])

(let [[value revision] (k/get store :head nil {:sync? true :with-revision? true})]
  (k/assoc store :head (update value :n inc)
           {:sync? true :expected-revision revision}))
;; => throws ex-info with {:type :konserve/revision-mismatch} if someone else
;;    committed in between; re-read and retry.
```

`konserve.core/absent` as the expected revision means *create it only if it does
not exist*.

The comparison is made by the database, in the same statement as the write:

``` sql
UPDATE konserve SET header = ?, meta = ?, val = ? WHERE id = ? AND meta = ?
```

If no row matches, nothing was written and the caller is told. Create-if-absent
is a plain `INSERT` refused by the primary key. There is no version column and
no schema change — an existing table works as it is.

**How far the fence reaches** depends on the database, not on this library, and
konserve exposes that as the store's conditional-write domain:

| Database | Domain | Orders writers |
|---|---|---|
| PostgreSQL, YugabyteDB, MySQL, SQL Server | `:global` | anywhere that can reach the server |
| H2 over `tcp://` or `ssl://` | `:global` | anywhere that can reach the server |
| SQLite, file-based H2 | `:machine` | processes on the host holding the file |
| H2 `mem:`, SQLite `:memory:` | `:process` | one runtime |

The domain is read from the deployment, not just the `:dbtype`, because one
dbtype spells three different things: an embedded H2 file orders every process
on the host, an H2 `mem:` database is invisible to the process next door, and an
H2 server orders writers anywhere on the network.

A `:dbtype` this backend does not recognise gets no domain, and
`:expected-revision` is refused rather than silently ignored. SQLite over a
network filesystem is `:machine` at best — its own documentation calls locking
there unreliable.

### Limits

- **Existing keys must be written once before they can be fenced.** The revision
  lives in konserve's metadata and was introduced with this feature, so a key
  last written by an older release has none. Fencing it raises
  `:konserve/revision-unavailable` — a deliberate refusal rather than a guessed
  "unchanged". One ordinary write gives the key a revision; the table itself
  needs no migration.
- **Binary values cannot be fenced.** `konserve.core/bassoc` refuses
  `:expected-revision` in konserve itself, so a pointer stored with `bassoc` gets
  no fencing on any backend.
- **Multi-key operations cannot be fenced.** `multi-assoc` and `multi-dissoc`
  both refuse the option; there is nothing coherent to compare across a batch.
- **`:config {:in-place? false}` is refused at connect time.** That layout writes
  `<key>.new` and renames it into place, but a rename here is `UPDATE ... SET id`
  and the primary key rejects it whenever the destination exists — so the second
  write to any key failed, with or without fencing. It is now an error rather
  than a surprise.
- **`:jdbcUrl` with H2 does not work** (pre-existing): `connection/uri->db-spec`
  returns no `:dbtype` for `jdbc:h2:` URLs. Use `:dbtype "h2"` with `:dbname`.

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

## License

Copyright © 2021-2026 Alexander Oloo, Christian Weilbach, Judith Massa

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
