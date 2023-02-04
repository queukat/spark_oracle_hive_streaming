name := "OracleToHiveMigrator"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.apache.spark" %% "spark-hive" % "3.2.2",
  "com.oracle.database.jdbc" % "ojdbc8" % "21.6.0.0.1"
)


resolvers ++= Seq(
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)
