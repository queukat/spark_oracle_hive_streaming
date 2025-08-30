package queukat.spark_universal

import org.apache.spark.SparkException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * A class designed for interacting with Hive.
 *
 * @constructor create a new [[HiveManager]] with a [[SparkSession]].
 * @param spark the [[SparkSession]] used to execute queries.
 */
class HiveManager(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Method to convert a schema to a string of DDL statements.
   *
   * @param schema the schema to be converted.
   * @return a string of DDL statements.
   */
  def schemaToDDL(schema: StructType): String = {
    schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
  }

  /**
   * Method to create a new Hive table if it does not already exist.
   *
   * @param hivetable        the name of the Hive table.
   * @param schema           the schema for the new table.
   * @param partitionColumns the columns used for partitioning the table.
   */
  def createHiveTableIfNotExists(hivetable: String, schema: StructType, partitionColumns: Seq[String] = Seq()): Unit = {
    try {
      // Create the base table
      val tableSchemaWithoutPartitions = new StructType(schema.filterNot(field => partitionColumns.contains(field.name)).toArray)
      spark.sql(s"CREATE TABLE IF NOT EXISTS $hivetable (${schemaToDDL(tableSchemaWithoutPartitions)})")

      // Add the partitions if they exist
      partitionColumns.foreach(partitionColumn =>
        spark.sql(s"ALTER TABLE $hivetable ADD IF NOT EXISTS PARTITION ($partitionColumn)"))
    }
    catch {
      case e: Exception => logger.error(s"##### ERROR: An error occurred while creating the table: ${e.getMessage} #####")
        e.printStackTrace()
    }
  }

  /**
   * Method to save a DataFrame as a temporary table in Hive.
   *
   * @param df            the DataFrame to be saved.
   * @param tempTableName the name of the temporary table.
   * @param numPartitions the number of partitions.
   */
  def saveAsTemporaryTable(df: DataFrame, tempTableName: String, numPartitions: Int): Unit = {
    val partitionedDf = if (numPartitions > 0) df.repartition(numPartitions) else df
    try {
      partitionedDf.write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable(tempTableName)
    } catch {
      case e: SparkException if e.getMessage.contains("[LOCATION_ALREADY_EXISTS]") =>
        logger.error(s"##### WARNING: Location already exists. Attempting to delete and overwrite: ${e.getMessage} #####")
        try {
          spark.sql(s"DROP TABLE $tempTableName")
          partitionedDf.write
            .mode(SaveMode.Overwrite)
            .format("orc")
            .saveAsTable(tempTableName)
        } catch {
          case e: Exception =>
            logger.error(s"##### ERROR: An error occurred while trying to delete and overwrite the table: ${e.getMessage} #####")
            e.printStackTrace()
        }
      case e: Exception =>
        logger.error(s"##### ERROR: An error occurred while saving the temporary table: ${e.getMessage} #####")
        e.printStackTrace()
    }
  }

  /**
   * Method to insert data from a temporary table into a target Hive table.
   *
   * @param tempTableName    the name of the temporary table.
   * @param newHiveTable     the name of the target Hive table.
   * @param hiveSchema       the schema for the new table.
   * @param partitionColumns the columns used for partitioning the table.
   */
  def insertDataIntoHiveTable(tempTableName: String, newHiveTable: String, hiveSchema: StructType, partitionColumns: Seq[String] = Seq.empty): Unit = {
    try {
      val partitionDDL = if (partitionColumns.nonEmpty) s"PARTITIONED BY (${partitionColumns.mkString(", ")})" else ""
      spark.sql(s"CREATE TABLE $newHiveTable (${hiveSchema.toDDL}) $partitionDDL")
      spark.sql(s"INSERT OVERWRITE TABLE $newHiveTable SELECT * FROM $tempTableName")
    }
    catch {
      case e: Exception => logger.error(s"##### ERROR: An error occurred while inserting data into the Hive table: ${e.getMessage} #####")
        e.printStackTrace()
    }
  }

  /**
   * Method to drop a temporary table in Hive.
   *
   * @param tempTableName the name of the temporary table to be dropped.
   */
  def dropTemporaryTable(tempTableName: String): Unit = {
    try {
      spark.sql(s"DROP TABLE $tempTableName")
    }

    catch {
      case e: Exception =>
        logger.error(s"##### ERROR: An error occurred while dropping the temporary table: ${e.getMessage} #####")
        e.printStackTrace()
    }
  }

  /**
   * Method to create partitions and subpartitions in a Hive table.
   *
   * @param hivetable        the name of the Hive table.
   * @param partitionName    the name of the partition.
   * @param subpartitionName the name of the subpartition.
   */
  def createPartitionsAndSubpartitions(hivetable: String, partitionName: String, subpartitionName: String): Unit = {
    try {
      if (partitionName != null) {
        spark.sql(s"ALTER TABLE $hivetable ADD IF NOT EXISTS PARTITION (partition_name='$partitionName')")
        if (subpartitionName != null) {
          spark.sql(s"ALTER TABLE $hivetable ADD IF NOT EXISTS PARTITION (partition_name='$partitionName', subpartition_name='$subpartitionName')")
        }
      }
    }
    catch {
      case e: Exception =>
        logger.error(s"##### ERROR: An error occurred while creating partitions and subpartitions: ${e.getMessage} #####")
        e.printStackTrace()
    }
  }
}