package queukat.spark_universal

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Interacts with Hive tables through Spark SQL.
 */
class HiveManager(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def quoteTableName(tableName: String): String = {
    tableName.split("\\.").map(part => s"`${part.replace("`", "``")}`").mkString(".")
  }

  def createHiveTableIfNotExists(hivetable: String, schema: StructType, partitionColumns: Seq[String] = Seq.empty): Unit = {
    try {
      val quotedTableName = quoteTableName(hivetable)
      val tableSchemaWithoutPartitions = new StructType(schema.filterNot(field => partitionColumns.contains(field.name)).toArray)
      val partitionDDL =
        if (partitionColumns.isEmpty) ""
        else {
          val partitionFields = partitionColumns.map { partitionColumn =>
            val field = schema.find(_.name == partitionColumn).getOrElse {
              throw new IllegalArgumentException(s"Partition column $partitionColumn not found in schema.")
            }
            s"`${partitionColumn.replace("`", "``")}` ${field.dataType.sql}"
          }.mkString(", ")
          s" PARTITIONED BY ($partitionFields)"
        }

      spark.sql(s"CREATE TABLE IF NOT EXISTS $quotedTableName (${tableSchemaWithoutPartitions.toDDL})$partitionDDL")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create Hive table $hivetable: ${e.getMessage}", e)
        throw e
    }
  }

  def saveAsTemporaryTable(df: DataFrame, tempTableName: String, numPartitions: Int): Unit = {
    val partitionedDf = if (numPartitions > 0) df.repartition(numPartitions) else df
    try {
      partitionedDf.write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable(quoteTableName(tempTableName))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to save temporary table $tempTableName: ${e.getMessage}", e)
        throw e
    }
  }

  def insertDataIntoHiveTable(tempTableName: String, newHiveTable: String, hiveSchema: StructType, partitionColumns: Seq[String] = Seq.empty): Unit = {
    try {
      val quotedTarget = quoteTableName(newHiveTable)
      val quotedTemp = quoteTableName(tempTableName)
      createHiveTableIfNotExists(newHiveTable, hiveSchema, partitionColumns)
      spark.sql(s"INSERT OVERWRITE TABLE $quotedTarget SELECT * FROM $quotedTemp")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to insert data into Hive table $newHiveTable: ${e.getMessage}", e)
        throw e
    }
  }

  def dropTemporaryTable(tempTableName: String): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS ${quoteTableName(tempTableName)}")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to drop temporary table $tempTableName: ${e.getMessage}", e)
        throw e
    }
  }

  def createPartitionsAndSubpartitions(hivetable: String, partitionName: String, subpartitionName: String): Unit = {
    try {
      val quotedTableName = quoteTableName(hivetable)
      if (partitionName != null) {
        spark.sql(s"ALTER TABLE $quotedTableName ADD IF NOT EXISTS PARTITION (partition_name='$partitionName')")
        if (subpartitionName != null) {
          spark.sql(s"ALTER TABLE $quotedTableName ADD IF NOT EXISTS PARTITION (partition_name='$partitionName', subpartition_name='$subpartitionName')")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create partitions and subpartitions for $hivetable: ${e.getMessage}", e)
        throw e
    }
  }
}
