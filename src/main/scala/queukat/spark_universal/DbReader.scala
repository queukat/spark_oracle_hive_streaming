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

  logger.info(
    s"${MigrationLogging.stage("JDBC")} DbReader initialized " +
      s"${MigrationLogging.kv("numPartitions", numPartitions)} " +
      s"${MigrationLogging.kv("fetchSize", fetchSize)}"
  )

  def readFromJDBC(query: String): DataFrame = {
    logger.info(s"${MigrationLogging.stage("JDBC")} Preparing JDBC read ${MigrationLogging.sqlPreview(query)}")
    val jdbcOptions = baseJdbcOptions + ("dbtable" -> s"(${query}) oracle_to_hive_src")
    val df = spark.read.format("jdbc").options(jdbcOptions).load()
    logger.info(
      s"${MigrationLogging.success("JDBC")} JDBC DataFrame ready " +
        s"${MigrationLogging.kv("columns", df.columns.length)}"
    )
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
    logger.info(s"${MigrationLogging.success("SCN")} Captured Oracle snapshot ${MigrationLogging.kv("scn", snapshotScn)}")
    snapshotScn
  }

  def loadData(queryIterator: Iterator[String], fallbackSchema: StructType): DataFrame = {
    val queries = queryIterator.toVector
    logger.info(s"${MigrationLogging.stage("LOAD")} Building source DataFrames ${MigrationLogging.kv("segments", queries.size)}")

    if (queries.nonEmpty) {
      logger.info(s"${MigrationLogging.stage("LOAD")} First rowid range ${MigrationLogging.sqlPreview(queries.head)}")
    }

    val dataFrames = queries.map(readFromJDBC)

    if (dataFrames.isEmpty) {
      logger.info(
        s"${MigrationLogging.warning("LOAD")} No Oracle partitions were generated. " +
          s"Returning empty DataFrame ${MigrationLogging.kv("columns", fallbackSchema.fields.length)}"
      )
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], fallbackSchema)
    } else {
      val merged = dataFrames.reduce(_.unionByName(_))
      logger.info(
        s"${MigrationLogging.success("LOAD")} Combined JDBC partitions " +
          s"${MigrationLogging.kv("frames", dataFrames.size)} " +
          s"${MigrationLogging.kv("columns", merged.columns.length)}"
      )
      merged
    }
  }

  def close(): Unit = {
    logger.info(s"${MigrationLogging.stage("JDBC")} DbReader closed")
  }
}
