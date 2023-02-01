name := "OracleToHiveMigrator"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-hive" % "3.0.0",
  "com.oracle.jdbc" % "ojdbc8" % "19.7.0.0"
)

