package queukat.spark_universal

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Reads Oracle data through Spark's JDBC reader.
 *
 * The reader intentionally keeps query execution on the Spark side to avoid
 * materializing entire result sets on the driver.
 */
class DbReader(
  private val spark: SparkSession,
  private val url: String,
  private val oracleUser: String,
  private val oraclePassword: String,
  private val numPartitions: Int,
  private val fetchSize: Int
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val baseJdbcOptions = Map(
    "url" -> url,
    "user" -> oracleUser,
    "password" -> oraclePassword,
    "fetchSize" -> fetchSize.toString,
    "numPartitions" -> numPartitions.toString,
    "driver" -> "oracle.jdbc.OracleDriver"
  )

  logger.info("DbReader created")

  def readFromJDBC(query: String): DataFrame = {
    logger.info(s"Starting readFromJDBC with query: $query")
    val jdbcOptions = baseJdbcOptions + ("dbtable" -> s"(${query}) oracle_to_hive_src")
    val df = spark.read.format("jdbc").options(jdbcOptions).load()
    logger.info("readFromJDBC completed successfully")
    df
  }

  def captureSnapshotScn(): Long = {
    val scnQuery = "SELECT dbms_flashback.get_system_change_number AS CURRENT_SCN FROM dual"
    val snapshotRow = readFromJDBC(scnQuery).first()
    val snapshotScn = snapshotRow.get(0) match {
      case n: java.math.BigDecimal => n.longValueExact()
      case n: java.lang.Number => n.longValue()
      case other => other.toString.toLong
    }
    logger.info(s"Captured Oracle snapshot SCN: $snapshotScn")
    snapshotScn
  }

  def loadData(queryIterator: Iterator[String], fallbackSchema: StructType): DataFrame = {
    logger.info("Starting loadData method")
    val dataFrames = queryIterator.toList.map(readFromJDBC)

    if (dataFrames.isEmpty) {
      logger.info("No Oracle data partitions were generated. Returning an empty DataFrame with the fallback schema.")
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], fallbackSchema)
    } else {
      dataFrames.reduce(_.unionByName(_))
    }
  }

  def close(): Unit = {
    logger.info("DbReader closed")
  }
}
