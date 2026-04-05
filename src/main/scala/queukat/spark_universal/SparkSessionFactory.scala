package queukat.spark_universal

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionFactory {
  private val logger = LoggerFactory.getLogger(SparkSessionFactory.getClass)

  def getSparkSession(appName: String = "OracleToHiveMigrator", master: String = sys.props.getOrElse("spark.master", "local[*]")): SparkSession = {
    logger.info(s"${MigrationLogging.stage("SPARK")} Preparing Spark session ${MigrationLogging.kv("appName", appName)} ${MigrationLogging.kv("master", master)}")

    val session = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    logger.info(
      s"${MigrationLogging.success("SPARK")} Spark session ready " +
        s"${MigrationLogging.kv("appId", session.sparkContext.applicationId)} " +
        s"${MigrationLogging.kv("master", session.sparkContext.master)}"
    )

    session
  }
}
