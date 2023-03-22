import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ColumnInfo_1(columnName: String, dataType: String, dataPrecision: String, dataScale: String)

object NewSpark_1 {
  def migrate(
               url: String,
               oracleUser: String,
               oraclePassword: String,
               tableName: String,
               owner: String,
               hivetable: String
             ): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OracleToHiveMigrator")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    def readFromJDBC(query: String): DataFrame = {
      val jdbcOptions = Map(
        "url" -> url,
        "user" -> oracleUser,
        "password" -> oraclePassword,
        "dbtable" -> query,
        "sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;"""

      )
      spark.read.format("jdbc").options(jdbcOptions).load()
    }

    // SQL queries
    val schemaQuery = s"SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE " +
      s"FROM ALL_TAB_COLUMNS " +
      s"WHERE TABLE_NAME = '$tableName'"
    val fileIdsQuery = s"SELECT data_object_id, file_id, relative_fno, subobject_name, " +
      s"MIN(start_block_id) start_block_id, MAX(end_block_id) end_block_id, SUM(blocks) blocks " +
      s"FROM (SELECT o.data_object_id, o.subobject_name, e.file_id, e.relative_fno, e.block_id start_block_id, " +
      s"e.block_id + e.blocks - 1 end_block_id, e.blocks " +
      s"FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp " +
      s"WHERE o.owner = $owner AND o.object_name = $tableName AND e.owner = $owner AND e.segment_name = $tableName " +
      s"AND o.owner = e.owner AND o.object_name = e.segment_name " +
      s"AND (o.subobject_name = e.partition_name OR (o.subobject_name IS NULL AND e.partition_name IS NULL)) " +
      s"AND o.owner = tsp.table_owner(+) AND o.object_name = tsp.table_name(+) " +
      s"AND o.subobject_name = tsp.subpartition_name(+)) GROUP BY data_object_id, file_id, relative_fno, subobject_name"
    val dataQuery = s"SELECT /*+ NO_INDEX(t) */ $queryColumns FROM $owner.$tableName " +
      s"WHERE ((rowid >= dbms_rowid.rowid_create(1, $data_object_id, $relative_fno, $start_block_id, 0) " +
      s"AND rowid <= dbms_rowid.rowid_create(1, $data_object_id, $relative_fno, $end_block_id, 32767)))"


    val oracleSchema = readFromJDBC(schemaQuery)

    val castedSchema = oracleSchema
      .select(
        col("COLUMN_NAME").alias("columnName"),
        col("DATA_TYPE").alias("dataType"),
        col("DATA_PRECISION").alias("dataPrecision"),
        col("DATA_SCALE").alias("dataScale")
      )
      .as[ColumnInfo]

    def convertColumnInfoToStructField(info: ColumnInfo): StructField = {
      val hiveDataType = info.dataType match {
        case "VARCHAR2" => StringType
        case "DATE" => TimestampType
        case "NUMBER" =>
          if (info.dataPrecision == null || info.dataScale == null) {
            val maxLeftOfDecimalQuery = s"select max(abs(trunc(${info.columnName},0))) from $owner.$tableName"
            val maxRightOfDecimalQuery = s"select max(mod(${info.columnName}, 1)) from $owner.$tableName"
            val maxLeftOfDecimal = readFromJDBC(maxLeftOfDecimalQuery).first().length
            val maxRightOfDecimal = readFromJDBC(maxRightOfDecimalQuery).first().toString.substring(3).length
            if (maxLeftOfDecimal + maxRightOfDecimal > 38) {
              StringType
            } else {
              DecimalType(maxLeftOfDecimal, maxRightOfDecimal)
            }
          } else {
            DecimalType(info.dataPrecision.toInt, info.dataScale.toInt)
          }
        case _ => StringType
      }
      StructField(info.columnName, hiveDataType, nullable = true)
    }

    val hiveSchema = StructType(castedSchema.collect().map(convertColumnInfoToStructField))


    val fileIds = readFromJDBC(fileIdsQuery)

    fileIds.select("relative_fno", "data_object_id", "start_block_id", "end_block_id")
      .repartition(10)
      .foreach { row =>
        val relative_fno = row.getAs[Int]("relative_fno")
        val data_object_id = row.getAs[Int]("data_object_id")
        val start_block_id = row.getAs[Int]("start_block_id")
        val end_block_id = row.getAs[Int]("end_block_id")

        val queryColumns = schema
          .map(_.name)
          .mkString(", ")


        val oracleData = readFromJDBC(dataQuery)

        // Записываем данные во временную таблицу
        val tempTableName = s"temp_$tableName"
        oracleData.write
          .mode("overwrite")
          .format("parquet")
          .option("compression", "snappy")
          .saveAsTable(tempTableName)
      }

    val partitionsQuery = s"SELECT PARTITION_NAME, SUBPARTITION_NAME, COLUMN_NAME " +
      s"FROM DBA_TAB_SUBPARTITIONS WHERE TABLE_NAME = '$tableName' AND TABLE_OWNER = '$owner'"
    val partitionsInfo = readFromJDBC(partitionsQuery)

    def getPartitionDefinition(partitionsInfo: DataFrame): String = {
      if (partitionsInfo.count() > 0) {
        val partitionColumns = partitionsInfo.select("COLUMN_NAME").distinct().collect().map(_.getAs[String]("COLUMN_NAME"))
        partitionColumns.map(col => s"$col STRING").mkString(", ")
      } else {
        ""
      }
    }

    val partitionDefinition = getPartitionDefinition(partitionsInfo)

    if (partitionDefinition.nonEmpty) {
      spark.sql(s"CREATE TABLE $hivetable ($hiveSchema, $partitionDefinition) PARTITIONED BY ($partitionDefinition) STORED AS PARQUET")
    } else {
      spark.sql(s"CREATE TABLE $hivetable ($hiveSchema) STORED AS PARQUET")
    }

    createPartitionsAndSubpartitions(hivetable, partitionsInfo)

    // Вставляем данные из временной таблицы в основную таблицу
    spark.sql(s"INSERT INTO $hivetable SELECT * FROM $tempTableName")

    // Удаляем временную таблицу
    spark.sql(s"DROP TABLE $tempTableName")
  }
}
