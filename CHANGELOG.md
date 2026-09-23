# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/).

## [2.0.0] - 2026-09-23

### Breaking Changes

- **`DBMManager.executeQuery`**: the `executeQuery(sql, values, ...)` overload was replaced by
  `executeQuery(SQLQuery, ...)`. Callers building raw SQL + parameter lists must switch to
  constructing a `SQLQuery`. (`97f3f66`)
- **RRN handling now requires the `__RNN` identity column.** The `ROW_NUMBER()`-based fallback for
  Relative Record Number ordering was dropped in favor of a single `__RNN` identity-column strategy;
  files without it are no longer supported the same way. (`37f496c`, `c268587`)
- **`jt400` RRN operations fail fast.** CHAIN/SETLL/SETGT/READE/READPE by Relative Record Number on
  the `jt400` connector now throw `UnsupportedOperationException` instead of whatever behavior
  previously occurred. (`ff95974`)
- **PostgreSQL dialect is enabled by default**, and its opt-out environment variable was renamed to
  `RELOAD_DIALECT_ENABLED`. Deployments relying on the old variable name or the previous
  opt-in default must update their configuration. (`cff373d`)
- **Autocommit lifecycle reworked.** Connection-scoped autocommit was replaced by page-based reads
  with autocommit scoped to query execution, including rollback-on-failure for shared connections.
  Code relying on the old connection-wide autocommit scoping may observe different transaction
  boundaries. (`98b9d0c`, `cd0aef0`, `d9744ba`)

### Added

- RRN-as-output support (`Result.rrn`). (`8aa5bfa`)
- Relative Record Number CHAIN/SETLL/SETGT/READE/READPE support on unkeyed files. (`aa460a6`)
- `executeUpdate` for parameterized DML on `SQLDBMManager`. (`2ee19d7`)
- Generic `executeQuery` API on `DBMManager`. (`32b5e85`)
- PostgreSQL SETLL/SETGT support via the `SQLDialect` pattern. (`c0d84bc`)

### Changed

- RRN ordering now falls back to `FileMetadata.fields` instead of a view's `ORDER BY`. (`46f37e2`)
- `withQueryExecution` split into `beforeQuery`/`afterResultSetClose` hooks. (`d07d677`)

### Fixed

- Restored arrival-sequence order for plain `READ` on RRN-mode files. (`f878fbf`)
- Missing `__RNN` identity column is now tolerated instead of failing. (`c268587`)
- `__RNN` column probe failures are now logged instead of swallowed. (`f7fc536`)
- PostgreSQL transactions are now reference-counted across overlapping result sets. (`56a21a4`)
- `closeFile` now actually closes the opened `SQLDBFile`. (`b2e9b84`)
- Guard against abandoned PostgreSQL transactions. (`2231993`)

## [1.7.0] - 2026-06-16

Prior releases predate this changelog. See git tags `v1.2.0` through `v1.7.0` for history.

[2.0.0]: https://github.com/smeup/reload/compare/v1.7.0...v2.0.0
[1.7.0]: https://github.com/smeup/reload/releases/tag/v1.7.0
