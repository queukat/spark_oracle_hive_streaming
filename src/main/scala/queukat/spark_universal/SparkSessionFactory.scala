package queukat.spark_universal

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def getSparkSession(appName: String = "OracleToHiveMigrator", master: String = sys.props.getOrElse("spark.master", "local[*]")): SparkSession = {
    SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
}
