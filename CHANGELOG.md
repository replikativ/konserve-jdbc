# Changelog

All notable, user-visible changes to konserve-jdbc are documented here.

## Unreleased

### Added
- **Conditional writes (`:expected-revision`).** The backing implements konserve's
  `PConditionalWrite` and `PSelfConditionalWrite`: a fenced write is one
  `UPDATE ... WHERE id = ? AND meta = ?`, so the database compares and writes in the
  same statement, and create-if-absent is an `INSERT` refused by the primary key.
  No version column and no schema change — existing tables work unchanged. Losing
  the comparison yields `{:type :konserve/revision-mismatch}`.

  The domain follows the database: `:global` for PostgreSQL, YugabyteDB, MySQL and
  SQL Server, `:machine` for SQLite and file-based H2, `:process` for in-memory H2.
  A `:dbtype` outside that set gets no domain and `:expected-revision` is refused
  rather than ignored. Requires konserve `0.9.378`+.

  Two dialect details, both measured rather than assumed: SQL Server's `varbinary`
  comparison ignores trailing zero bytes, so the statement compares `DATALENGTH`
  too; and SQLite reports a duplicate key with no SQLSTATE at all, so the refusal
  is recognised by its vendor code as well as by SQLSTATE class 23.

  Existing tables work unchanged, but an existing KEY carries no revision until it
  is written once: the token is part of the metadata this konserve version
  introduced, and konserve refuses to fence a key that predates it
  (`:konserve/revision-unavailable`) rather than guess that it is unchanged.

  Not fenceable, and each says so rather than ignoring the option: `bassoc`
  (konserve's own policy for binary values), `multi-assoc`, and `multi-dissoc`.

- **Read-miss-safe reads (one SELECT, no probe).** The JDBC backing implements
  konserve's `PReadMissSafe` and `-read-header` throws `store-key-not-found-ex` when
  the row is absent (the SELECT returns no rows). On a konserve that supports the
  marker the redundant `-blob-exists?` SELECT probe is dropped, so a read is one
  SELECT, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip it too.
  Requires konserve `0.9.354`+.
- **Pool introspection and recovery.** `pool-status` returns a credential-free
  snapshot of the pool registry (`:refs`, `:open?`, `:db-spec`). `remove-from-pool`
  now forgets a pool *without* closing it, so the next `connect-store` builds a
  fresh one while live stores keep working, and `:validate-pool? true` in the store
  config makes `connect-store` probe an existing pool and rebuild it if something
  closed it out of band. `release-pool!` is the spec-level counterpart of `release`.

### Fixed
- **Releasing one store no longer closes the pool every other store is using.**
  Pools are keyed by connection, not by table, so every store on a database shares
  one — but `release` closed that pool outright, leaving every co-tenant holding a
  dead DataSource. Reported downstream, where datahike's `create-database` /
  `delete-database` release their store when finished and took every other tenant
  on the server down with them. Pools are now reference counted: `connect-store`
  takes a reference, `release` gives one back, and the pool closes when the last
  holder lets go. `release` is idempotent per store and returns `:closed`,
  `:retained`, `:already-released` or `:absent`; `{:force? true}` restores the old
  unconditional close for process shutdown. A connect that fails after taking
  its reference (a bad table name, a privilege error) hands the reference back,
  and a store whose pool was closed out of band and rebuilt releases as `:stale`
  rather than closing the pool the new holders are using.
- **Leaked JVM shutdown hooks.** Every pool registered a shutdown hook that was
  never removed, so each connect/release cycle leaked a hook thread and kept a
  closed DataSource reachable. Hooks are now removed when the pool is closed.
- **`delete-store` on a `:jdbcUrl`-only config used the wrong dialect.** It built
  its backing from the raw spec, so the `postgres` → `postgresql` normalisation in
  `prepare-spec` never reached the DROP statement.
- **`-delete-store` no longer closes a pooled DataSource.** It closed whatever
  connection the backing held, which is a `ClassCastException` on a pool and would
  have shut every other tenant out; it now closes only a connection it owns.

### Changed
- konserve `0.9.376` → `0.9.378`, which refuses `:expected-revision` on
  `multi-get` and `multi-dissoc` centrally (konserve#175) and revokes a
  self-fenced domain under `:in-place? false` (konserve#176) — both of which this
  backend already defended locally. The local defences stay: a backing should not
  depend on the library above it for a property it can enforce itself.
- **`:config {:in-place? false}` is now refused at connect time.** It never worked
  on this backend — the layout renames `<key>.new` over `<key>`, but a rename is
  `UPDATE ... SET id = ?` and the primary key refuses it whenever the destination
  row exists, so the second write to any key failed. It would also have bypassed
  the fence silently. An explicit error replaces both.
- `konserve-jdbc.core/->JDBCTable` takes a fourth positional argument (the
  conditional-write read cache). Code that constructs the record directly rather
  than through `connect-store` needs updating; `connect-store` itself is
  unchanged.
- konserve `0.9.342` → `0.9.378`.
- Pool creation happens behind a per-database delay and outside the registry lock,
  so a thousand tenants connecting concurrently no longer serialise behind one JDBC
  handshake. c3p0 sizing keys (`:maxPoolSize`, `:checkoutTimeout`, …) passed in the
  store config are applied when the pool is built; they are not part of the pool
  key, so the first store to reach a database sets them and a later store asking for
  different values is logged and ignored.
- `:sync?` is no longer part of the pool key. It never affected the pool itself,
  and keying on it gave one database two pools as soon as sync and async stores
  were mixed, and made `release-pool!` / `remove-from-pool` miss the pool when
  handed the caller's own spec. Per-store lifecycle state rides in `JDBCTable`'s
  metadata rather than a further positional field.
