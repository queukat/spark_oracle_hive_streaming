# Project: Spark Universal Migrator

Spark Universal Migrator is a Scala/Spark library for full-load table-by-table migration from Oracle to Hive. It captures an Oracle snapshot SCN, reads source rows through Spark JDBC using `ROWID` range queries built from Oracle extent metadata, applies an explicit Oracle-to-Spark schema policy, and writes the result into Hive through a temporary table plus `INSERT OVERWRITE`.

# Requirements

- Apache Spark 3.4
- Scala 2.12.x
- Oracle as the source system
- Hive metastore / Hive-enabled Spark as the target system
- Oracle JDBC driver support on the runtime classpath

# What It Supports

- Full-load data migration
- Table-by-table execution
- Range-based reads using Oracle `ROWID`
- Snapshot-based reads through captured SCN
- Oracle type handling modes:
  - `spark`: keep Spark JDBC inferred types for ambiguous Oracle `NUMBER`
  - `oracle`: profile Oracle `NUMBER` columns without precision/scale metadata
  - `skip`: avoid extra profiling and fall back to `StringType` for ambiguous `NUMBER`

# What It Does Not Do

- CDC / change capture
- Incremental load orchestration
- Schema evolution management across runs
- Environment-variable based configuration
- Standalone CLI entrypoint in this repository

# Testing

Run the test suite with:

```bash
sbt test
```

The test suite covers SQL generation, schema conversion, Spark session creation, Spark-side JDBC load composition, and Hive overwrite behavior.

# Using the Library

Use the `NewSpark.migrate(...)` API from your own application entrypoint:

```scala
import queukat.spark_universal.NewSpark

object ExampleMigration {
  def main(args: Array[String]): Unit = {
    NewSpark.migrate(
      url = "jdbc:oracle:thin:@//localhost:1521/ORCL",
      oracleUser = "your_oracle_username",
      oraclePassword = "your_oracle_password",
      tableName = "employees",
      owner = "HR",
      hivetable = "employees_hive",
      numPartitions = 8,
      fetchSize = 1000,
      typeCheck = "spark"
    )
  }
}
```

# Notes

- The library expects Oracle users with access to the required metadata views used to compute extent ranges.
- Unsupported Oracle types fail fast during schema conversion instead of being silently downgraded.
- Temporary Hive table names are generated uniquely per migration run.

# Publishing

The repository includes GitHub Actions workflows for CI and Maven Central publishing. CI runs `sbt test`, and publishing uses `sbt +publishSigned` on release.
