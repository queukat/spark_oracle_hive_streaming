# Remediation Plan

## Completed in this iteration

- Replaced the previous asynchronous/manual JDBC flow with a synchronous fail-fast Spark JDBC migration path.
- Removed driver-side `ResultSet` materialization from the main migration path.
- Added Oracle snapshot SCN capture and flashback-aware data query generation.
- Tightened SQL identifier quoting for generated Oracle queries.
- Reworked Oracle schema conversion with explicit type handling and safer `NUMBER` behavior.
- Made Hive writes fail-fast and rerunnable by using `CREATE TABLE IF NOT EXISTS` plus `INSERT OVERWRITE`.
- Added tests for SQL generation, schema conversion, Spark session creation, `DbReader` load behavior, and Hive overwrite behavior.
- Updated README so runtime expectations match the actual library behavior.

## Remaining follow-up work

- Add Oracle integration tests against a real Oracle instance or compatible test environment.
- Add end-to-end reconciliation checks for source vs target row counts and optional checksums.
- Externalize runtime configuration into a typed config model instead of raw method parameters.
- Add metrics and structured run manifests for operability.
- Revisit the `dba_*` metadata dependency and reduce privilege requirements if the deployment model allows it.
