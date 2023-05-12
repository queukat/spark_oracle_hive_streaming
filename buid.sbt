
name := "OracleToHiveMigrator"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-hive" % "3.0.0",
  "com.oracle.database.jdbc" % "ojdbc8" % "21.6.0.0.1"
)

developers := List(
  Developer(
    id = "queukat",
    name = "yaroslav",
    email = "queukat@gmail.com",
    url = url("https://github.com/queukat")
  )
)

resolvers ++= Seq(
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
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
