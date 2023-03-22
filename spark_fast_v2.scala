import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object OracleToHive_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Oracle to Hive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val schemaName = "your_schema_name"
    val tableName = "your_table_name"
    val hiveTableName = "your_hive_table_name"
    val numSessions = 10

    val rowIdRange = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@//your_oracle_host:1521/your_service_name")
      .option("user", "your_username")
      .option("password", "your_password")
      .option("sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""")
      .option("dbtable", s"(SELECT " +
        s"MIN(dbms_rowid.rowid_to_absolute_fno(rowid, '$schemaName', '$tableName')) AS min_file, " +
        s"MAX(dbms_rowid.rowid_to_absolute_fno(rowid, '$schemaName', '$tableName')) AS max_file, " +
        s"MIN(dbms_rowid.rowid_block_number(rowid)) AS min_block, " +
        s"MAX(dbms_rowid.rowid_block_number(rowid)) AS max_block " +
        s"FROM $tableName)")
      .load()
      .first()

    val minFile = rowIdRange.getAs[BigDecimal]("min_file").intValue
    val maxFile = rowIdRange.getAs[BigDecimal]("max_file").intValue
    val minBlock = rowIdRange.getAs[BigDecimal]("min_block").intValue
    val maxBlock = rowIdRange.getAs[BigDecimal]("max_block").intValue

    val fileRange = (maxFile - minFile) / numSessions
    val blockRange = (maxBlock - minBlock) / numSessions

    implicit val ec: ExecutionContext = ExecutionContext.global

    val dataFrameFutures = (0 until numSessions).map { i =>
      Future {
        val startFile = minFile + i * fileRange
        val endFile = if (i == numSessions - 1) maxFile else startFile + fileRange - 1
        val startBlock = minBlock + i * blockRange
        val endBlock = if (i == numSessions - 1) maxBlock else startBlock + blockRange - 1

        val partitionQuery = s"(SELECT /*+ NO_INDEX(t) */ * FROM $tableName " +
          s"WHERE dbms_rowid.rowid_to_absolute_fno(rowid, '$schemaName' , '$tableName') " +
          s"BETWEEN $startFile AND $endFile " +
          s"AND dbms_rowid.rowid_block_number(rowid) BETWEEN $startBlock AND $endBlock)"

        spark.read
          .format("jdbc")
          .option("url", "jdbc:oracle:thin:@//your_oracle_host:1521/your_service_name")
          .option("user", "your_username")
          .option("password", "your_password")
          .option("sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""")
          .option("dbtable", partitionQuery)
          .option("fetchSize", "10000")
          .load()
      }
    }

    val dataFrames = Await.result(Future.sequence(dataFrameFutures), Duration.Inf)
    val allData = dataFrames.reduce(_ union _)

    allData.write.mode("overwrite").saveAsTable(hiveTableName)

    spark.stop()
  }
}
