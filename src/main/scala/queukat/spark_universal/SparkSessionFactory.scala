package queukat.spark_universal

import org.apache.spark.sql.SparkSession

/**
 * Singleton object to fetch a SparkSession
 *
 * @example To fetch a SparkSession with Hive support:
 * {{{
 * SparkSessionFactory.getSparkSession
 * }}}
 */
object SparkSessionFactory {

  /**
   * Fetches or creates a SparkSession with Hive support.
   * If a SparkSession has been already created, it will return the existing one.
   *
   * @param appName The name of the application.
   * @return A SparkSession with Hive support.
   */
  def getSparkSession(appName: String = "OracleToHiveMigrator", master: String = "yarn"): SparkSession = {
    SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
}
