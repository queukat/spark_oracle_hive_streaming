package queukat.spark_universal

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//The HiveManager class is designed to manage operations in Hive. It provides the following functionality:
//
//createHiveTableIfNotExists(hivetable: String, schema: StructType): This method creates a table in Hive if it does not exist. It takes the table name and schema as input parameters and uses this data to create a table.
//
//saveAsTemporaryTable(df: DataFrame, tempTableName: String, numPartitions: Int): This method saves the DataFrame as a temporary table in Hive. It takes DataFrame, temporary table name and number of partitions as input parameters.
//
//insertDataIntoHiveTable(tempTableName: String, hivetable: String): This method inserts data from a temporary table into the target Hive table. It takes the name of the temporary table and the name of the target table as input parameters.
//
//dropTemporaryTable(tempTableName: String): This method deletes the temporary table. It takes the name of the temporary table as an input parameter.
//
//createPartitionsAndSubpartitions(hivetable: String, partitionName: String, subpartitionName: String): This method creates sections and subsections in the Hive table. It takes the name of the table, the name of the section and the name of the subsection as input parameters. If the name of the section or subsection is not specified, the method does not create the corresponding section or subsection.
//
//The SparkSession object passed to the constructor of the HiveManager class is used to perform all operations in Hive.

class HiveManager(spark: SparkSession) {
  //Метод создает таблицу Hive, если она не существует.
  def createHiveTableIfNotExists(hivetable: String, schema: StructType): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $hivetable ${schema.toDDL}")
  }

  //The method saves the DataFrame as a temporary table in Hive.
  def saveAsTemporaryTable(df: DataFrame, tempTableName: String, numPartitions: Int): Unit = {
    df.repartition(numPartitions).write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable(tempTableName)
  }

  //The method inserts data from the temporary table into the target Hive table.
  def insertDataIntoHiveTable(tempTableName: String, hivetable: String): Unit = {
    spark.sql(s"INSERT INTO $hivetable SELECT * FROM $tempTableName")
  }

  //The method deletes the temporary table.
  def dropTemporaryTable(tempTableName: String): Unit = {
    spark.sql(s"DROP TABLE $tempTableName")
  }

  //The method creates sections and subsections in the Hive table.
  def createPartitionsAndSubpartitions(hivetable: String, partitionName: String, subpartitionName: String): Unit = {
    if (partitionName != null) {
      spark.sql(s"ALTER TABLE $hivetable ADD IF NOT EXISTS PARTITION (partition_name='$partitionName')")
      if (subpartitionName != null) {
        spark.sql(s"ALTER TABLE $hivetable ADD IF NOT EXISTS PARTITION (partition_name='$partitionName', subpartition_name='$subpartitionName')")
      }
    }
  }
}
