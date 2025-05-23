package queukat.spark_universal

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

/**
 * The `NewSpark` object provides methods to migrate data from an Oracle database to Hive.
 * It uses:
 *   - [[DbReader]] to read data from Oracle,
 *   - [[QueryGenerator]] to generate the necessary SQL queries for reading data and schema from Oracle,
 *   - [[SchemaConverter]] to convert the Oracle schema to Hive schema,
 *   - [[HiveManager]] to handle operations related to Hive (like writing the DataFrame to a Hive table).
 */
object NewSpark {

  private val logger = LoggerFactory.getLogger(NewSpark.getClass)

  private def createComponents(url: String,
                               oracleUser: String,
                               oraclePassword: String,
                               owner: String,
                               tableName: String,
                               numPartitions: Int,
                               fetchSize: Int,
                               typeCheck: String): (DbReader, SchemaConverter, HiveManager) = {
    logger.info("Getting Spark session.")
    val spark = SparkSessionFactory.getSparkSession()

    logger.info("Initializing DBReader.")
    val dbReader = new DbReader(spark, url, oracleUser, oraclePassword, numPartitions, fetchSize)

    logger.info("Initializing SchemaConverter.")
    val schemaConverter = new SchemaConverter(spark, owner, tableName, typeCheck, dbReader)

    logger.info("Initializing HiveManager.")
    val hiveManager = new HiveManager(spark)

    (dbReader, schemaConverter, hiveManager)
  }

  private def fetchSchema(dbReader: DbReader, tableName: String, owner: String) = {
    logger.info("Generating schema query.")
    val schemaQuery = QueryGenerator.generateSchemaQuery(tableName, owner)
    logger.info(s"Schema query: $schemaQuery")

    logger.info("Reading schema from Oracle database.")
    dbReader.readFromJDBC(schemaQuery)
  }

  private def prepareDataQueries(dbReader: DbReader, oracleSchema: org.apache.spark.sql.DataFrame, owner: String, tableName: String) = {
    logger.info("Generating universal query.")
    val universalQuery = QueryGenerator.generateUniversalQuery(owner, tableName)
    logger.info(s"Universal query: $universalQuery")

    logger.info("Getting partition info.")
    val partitionInfo = dbReader.readFromJDBC(universalQuery)

    logger.info("Getting query columns.")
    val queryColumns = oracleSchema.select("COLUMN_NAME").rdd.map(r => r(0)).collect().mkString(", ")
    logger.info(s"Query columns: $queryColumns")

    QueryGenerator.generateDataQuery(queryColumns, owner, tableName, partitionInfo).toIterator
  }

  private def handleLoadedData(df: org.apache.spark.sql.DataFrame,
                               hiveManager: HiveManager,
                               schemaConverter: SchemaConverter,
                               hivetable: String,
                               numPartitions: Int,
                               typeCheck: String): Unit = {
    val tempTableName = s"${hivetable}_temp"

    logger.info("Saving as temporary table.")
    hiveManager.saveAsTemporaryTable(df, tempTableName, numPartitions)

    logger.info("Analyzing and converting schema.")
    if (typeCheck == "spark") {
      val analyzedSchema = schemaConverter.analyzeAndConvertSchema(df)
      logger.info("Inserting data into Hive table WITH ANALYZE")
      hiveManager.insertDataIntoHiveTable(tempTableName, hivetable, StructType(analyzedSchema))
    } else {
      logger.info("Inserting data into Hive table.")
      hiveManager.insertDataIntoHiveTable(tempTableName, hivetable, df.schema)
    }

    logger.info("Dropping temporary table.")
    hiveManager.dropTemporaryTable(tempTableName)
  }

  /**
   * Migrates data from an Oracle database to a Hive table.
   *
   * This method performs the following steps:
   *   - Reads the schema from Oracle,
   *   - Converts the Oracle schema to a Hive schema,
   *   - Reads the data from Oracle,
   *   - If the `typeCheck` parameter is set to "spark", it analyzes the data types in the DataFrame and performs additional conversions if needed,
   *   - Writes the data to a Hive table.
   *
   * @param url The JDBC URL for the Oracle database.
   * @param oracleUser The Oracle database user.
   * @param oraclePassword The Oracle database password.
   * @param tableName The name of the Oracle table to migrate.
   * @param owner The owner of the Oracle table.
   * @param hivetable The name of the Hive table where the data will be written.
   * @param numPartitions The number of partitions to use when reading the Oracle data into a DataFrame.
   * @param fetchSize The number of rows to fetch at a time from Oracle.
   * @param typeCheck Strategy to handle Oracle NUMBER data types. If set to "spark", Spark will decide the data type, otherwise the Oracle data type will be used.
   * @throws Exception if there is a problem reading data from Oracle, converting the data types, or writing the data to Hive.
   */
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

    try {
      ResourceCleanup.start()
      val (dbReader, schemaConverter, hiveManager) =
        createComponents(url, oracleUser, oraclePassword, owner, tableName, numPartitions, fetchSize, typeCheck)

      val oracleSchema = fetchSchema(dbReader, tableName, owner)

      val dataQueries = prepareDataQueries(dbReader, oracleSchema, owner, tableName)

      logger.info("Loading data from Oracle database.")
      implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global
      dbReader
        .loadData(dataQueries, new StructType(schemaConverter.convert(oracleSchema).toArray))
        .onComplete {
          case Success(df) =>
            val numRows = df.count()
            logger.info(s"Data loaded successfully. Number of rows: $numRows")
            handleLoadedData(df, hiveManager, schemaConverter, hivetable, numPartitions, typeCheck)
          case Failure(e) =>
            logger.info(s"Failed to load data from Oracle to Hive. Reason: ${e.getMessage}")
            e.printStackTrace()
            throw e
        }

    } catch {
      case e: Exception => logger.error(s"Migration failed due to:{}", e.getMessage)
        e.printStackTrace()
        throw e
    } finally {
      ResourceCleanup.stop()
    }
    logger.info("Migration finished.")
  }
}
