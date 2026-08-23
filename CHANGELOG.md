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
