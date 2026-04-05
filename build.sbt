organization := "io.github.queukat"

name := "OracleToHiveMigrator"

homepage := Some(url("https://github.com/queukat/spark_oracle_hive_streaming"))

version := "2.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.7"

versionScheme := Some("early-semver")

Test / parallelExecution := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.oracle.database.jdbc" % "ojdbc8" % "21.9.0.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

developers := List(
  Developer(
    id = "queukat",
    name = "yaroslav",
    email = "queukat@gmail.com",
    url = url("https://github.com/queukat")
  )
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/queukat/spark_oracle_hive_streaming"),
    "scm:git@github.com:queukat/spark_oracle_hive_streaming.git"
  )
)

// Use the default Maven Central resolver; custom resolvers were removed
// because some pointed to non-repository URLs which caused invalid
// artifacts to be downloaded during dependency resolution.

publishMavenStyle := true

publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := { _ => false }


description := """
The Spark Universal Migrator is a Scala/Spark library for full-load Oracle-to-Hive table migration.
It captures a source snapshot SCN, reads Oracle rows through Spark JDBC using ROWID range queries,
converts Oracle schema metadata into a Spark/Hive-compatible schema, and writes the result into Hive
through a temporary table plus INSERT OVERWRITE.
"""

licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
