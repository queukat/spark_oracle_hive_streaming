name := "OracleToHiveMigrator"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-hive" % "3.0.0",
  "com.oracle.jdbc" % "ojdbc8" % "19.7.0.0"
)


resolvers ++= Seq(
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)
