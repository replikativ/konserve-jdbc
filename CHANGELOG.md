# Changelog

All notable, user-visible changes to konserve-jdbc are documented here.

## Unreleased

### Added
- **Read-miss-safe reads (one SELECT, no probe).** The JDBC backing implements
  konserve's `PReadMissSafe` and `-read-header` throws `store-key-not-found-ex` when
  the row is absent (the SELECT returns no rows). On a konserve that supports the
  marker the redundant `-blob-exists?` SELECT probe is dropped, so a read is one
  SELECT, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip it too.
  Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.354`.
