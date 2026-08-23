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
  rather than ignored. Requires konserve `0.9.376`+.

  Two dialect details, both measured rather than assumed: SQL Server's `varbinary`
  comparison ignores trailing zero bytes, so the statement compares `DATALENGTH`
  too; and SQLite reports a duplicate key with no SQLSTATE at all, so the refusal
  is recognised by its vendor code as well as by SQLSTATE class 23.

- **Read-miss-safe reads (one SELECT, no probe).** The JDBC backing implements
  konserve's `PReadMissSafe` and `-read-header` throws `store-key-not-found-ex` when
  the row is absent (the SELECT returns no rows). On a konserve that supports the
  marker the redundant `-blob-exists?` SELECT probe is dropped, so a read is one
  SELECT, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip it too.
  Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.376`.
