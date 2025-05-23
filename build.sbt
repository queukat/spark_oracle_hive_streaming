organization := "io.github.queukat"

name := "OracleToHiveMigrator"

homepage := Some(url("https://github.com/queukat/spark_oracle_hive_streaming"))

version := "2.0"

scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"

versionScheme := Some("early-semver")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.oracle.database.jdbc" % "ojdbc8" % "21.9.0.0",
  "com.oracle.database.jdbc" % "ucp" % "21.9.0.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
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

resolvers ++= Seq(
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Sonatype Nexus" at "https://nexus.example.com/repository/maven-public/",
  "JFrog Artifactory" at "https://artifactory.example.com/artifactory/public-repo/",
  "MavenRepository" at  "https://mvnrepository.com/"

)

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
The Spark Universal Migrator is a data migration Spark application that fetches data from an Oracle database and stores it in a Hive database.
It utilizes JDBC to connect to Oracle, processes and converts the data into a Hive-compatible schema, and then transfers it to Hive.
The migration process employs iterators to generate queries for data retrieval and utilizes a BlockingQueue in conjunction with a ThreadPoolExecutor for parallel processing of data.
Additionally, the application incorporates an Oracle connection pool to efficiently manage and reuse database connections, further optimizing performance and resource utilization during the migration process.
"""

licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
