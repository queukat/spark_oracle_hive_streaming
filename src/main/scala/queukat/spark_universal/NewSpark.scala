package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util.UUID

object NewSpark {
  private val logger = LoggerFactory.getLogger(NewSpark.getClass)

  private def createComponents(
    url: String,
    oracleUser: String,
    oraclePassword: String,
    owner: String,
    tableName: String,
    numPartitions: Int,
    fetchSize: Int,
    typeCheck: String
  ): (DbReader, SchemaConverter, HiveManager, Long) = {
    logger.info("Getting Spark session.")
    val spark = SparkSessionFactory.getSparkSession()

    logger.info("Initializing DBReader.")
    val dbReader = new DbReader(spark, url, oracleUser, oraclePassword, numPartitions, fetchSize)

    logger.info("Capturing Oracle snapshot SCN.")
    val snapshotScn = dbReader.captureSnapshotScn()

    logger.info("Initializing SchemaConverter.")
    val schemaConverter = new SchemaConverter(spark, owner, tableName, typeCheck, dbReader, Some(snapshotScn))

    logger.info("Initializing HiveManager.")
    val hiveManager = new HiveManager(spark)

    (dbReader, schemaConverter, hiveManager, snapshotScn)
  }

  private def fetchSchema(dbReader: DbReader, tableName: String, owner: String): DataFrame = {
    logger.info("Generating schema query.")
    val schemaQuery = QueryGenerator.generateSchemaQuery(tableName, owner)
    logger.info(s"Schema query: $schemaQuery")

    logger.info("Reading schema from Oracle database.")
    dbReader.readFromJDBC(schemaQuery)
  }

  private def prepareDataQueries(
    dbReader: DbReader,
    oracleSchema: DataFrame,
    owner: String,
    tableName: String,
    snapshotScn: Long
  ): Iterator[String] = {
    logger.info("Generating extent metadata query.")
    val universalQuery = QueryGenerator.generateUniversalQuery(owner, tableName)
    logger.info(s"Extent metadata query: $universalQuery")

    logger.info("Reading extent metadata from Oracle database.")
    val partitionInfo = dbReader.readFromJDBC(universalQuery)

    logger.info("Collecting column list for source reads.")
    val queryColumns = oracleSchema.select("COLUMN_NAME").collect().map(_.getString(0)).toSeq
    logger.info(s"Columns selected for migration: ${queryColumns.mkString(", ")}")

    QueryGenerator.generateDataQuery(queryColumns, owner, tableName, partitionInfo, Some(snapshotScn)).toIterator
  }

  private def handleLoadedData(
    df: DataFrame,
    hiveManager: HiveManager,
    hivetable: String,
    numPartitions: Int,
    hiveSchema: StructType
  ): Unit = {
    val tempTableName = s"${hivetable}_temp_${UUID.randomUUID().toString.replace("-", "")}"
    logger.info(s"Saving data as temporary table $tempTableName.")
    hiveManager.saveAsTemporaryTable(df, tempTableName, numPartitions)

    try {
      logger.info(s"Inserting data into Hive table $hivetable.")
      hiveManager.insertDataIntoHiveTable(tempTableName, hivetable, hiveSchema)
    } finally {
      logger.info(s"Dropping temporary table $tempTableName.")
      hiveManager.dropTemporaryTable(tempTableName)
    }
  }

  def migrate(
    url: String,
    oracleUser: String,
    oraclePassword: String,
    tableName: String,
    owner: String,
    hivetable: String,
    numPartitions: Int,
    fetchSize: Int,
    typeCheck: String
  ): Unit = {
    var dbReader: DbReader = null

    try {
      val (reader, schemaConverter, hiveManager, snapshotScn) =
        createComponents(url, oracleUser, oraclePassword, owner, tableName, numPartitions, fetchSize, typeCheck)
      dbReader = reader

      val oracleSchema = fetchSchema(dbReader, tableName, owner)
      val targetSchema = StructType(schemaConverter.convert(oracleSchema).toArray)
      val dataQueries = prepareDataQueries(dbReader, oracleSchema, owner, tableName, snapshotScn)

      logger.info("Loading data from Oracle database.")
      val loadedDf = dbReader.loadData(dataQueries, targetSchema)

      val (dfToWrite, effectiveSchema) =
        if (typeCheck == "spark") {
          loadedDf -> loadedDf.schema
        } else {
          schemaConverter.castToSchema(loadedDf, targetSchema) -> targetSchema
        }

      logger.info("Writing migrated data to Hive.")
      handleLoadedData(dfToWrite, hiveManager, hivetable, numPartitions, effectiveSchema)
      logger.info("Migration finished successfully.")
    } catch {
      case e: Exception =>
        logger.error(s"Migration failed due to: ${e.getMessage}", e)
        throw e
    } finally {
      if (dbReader != null) {
        dbReader.close()
      }
    }
  }
}
