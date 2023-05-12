package queukat.spark_universal

// It uses DbReader to read data from the database, QueryGenerator to generate SQL queries and HiveManager to work with Hive.

// This method is used to migrate data from a database to Hive.
// The parameters "spark", "dbConfig", "tableName", "numPartitions" and "hivetable" represent SparkSession, database configuration, table name in the database, number of partitions for DataFrame and table name in Hive, respectively.
// The method first creates an instance of DbReader, SchemaConverter and HiveManager. Then it generates a schema query, reads the schema from the database, converts it and saves it as a temporary table in Hive. After that, it generates a data query, reads the data from the database, converts it and inserts it into the Hive table. After inserting the data, it deletes the temporary table.
// Please note that the application only supports Oracle data types that can be converted to Hive data types. Therefore, the application checks Oracle data types and removes any columns whose data types cannot be converted to Hive data types.
// Please also note that the application only supports Oracle data types that can be converted to Hive data types. Therefore, the application checks Oracle data types and removes any columns whose data types cannot be converted to Hive data types.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.concurrent.Await
import scala.concurrent.duration.Duration
//The object is responsible for the basic operations related to reading data from the database and storing it in Hive.
object NewSpark {
  def migrate(
               url: String,
               oracleUser: String,
               oraclePassword: String,
               tableName: String,
               owner: String,
               hivetable: String,
               numPartitions: Int,
               fetchSize: Int,
               skipTypeCheck: Boolean
             ): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OracleToHiveMigrator")
      .enableHiveSupport()
      .getOrCreate()

    val dbReader = new DbReader(spark, url, oracleUser, oraclePassword, numPartitions, fetchSize)
    val schemaConverter = new SchemaConverter(spark, owner, tableName, skipTypeCheck, dbReader)
    val hiveManager = new HiveManager(spark)

    val schemaQuery = QueryGenerator.generateSchemaQuery(tableName, owner)
    val oracleSchema = dbReader.readFromJDBC(schemaQuery)
    val futureHiveSchema = schemaConverter.convert(oracleSchema)

    val universalQuery = QueryGenerator.generateUniversalQuery(owner, tableName)
    val partitionInfo = dbReader.readFromJDBC(universalQuery)

    val queryColumns = oracleSchema.select("COLUMN_NAME").rdd.map(r => r(0)).collect().mkString(", ")

    val queries = QueryGenerator.generateDataQuery(queryColumns, owner, tableName, partitionInfo, numPartitions)

    val queryGroups = queries.grouped(numPartitions).toSeq

    val loadedDataFuture = dbReader.loadData(queryGroups)
    val loadedData = Await.result(loadedDataFuture, Duration.Inf)

    val hiveSchema = Await.result(futureHiveSchema, Duration.Inf)
    val hiveStructType = StructType(hiveSchema)
    hiveManager.createHiveTableIfNotExists(hivetable, hiveStructType)
    val tempTableName = hivetable + "_temp"

    val tempDF = spark.createDataFrame(loadedData.rdd, hiveStructType)
    hiveManager.saveAsTemporaryTable(tempDF, tempTableName, numPartitions)

    hiveManager.insertDataIntoHiveTable(tempTableName, hivetable)

    hiveManager.dropTemporaryTable(tempTableName)

    spark.stop()
  }
}