package queukat.spark_universal

import org.apache.spark.sql.types.StructType
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
      logger.info("Getting Spark session.")
      val spark = SparkSessionFactory.getSparkSession()

      logger.info("Initializing DBReader.")
      val dbReader = new DbReader(spark, url, oracleUser, oraclePassword, numPartitions, fetchSize)

      logger.info("Initializing SchemaConverter.")
      val schemaConverter = new SchemaConverter(spark, owner, tableName, typeCheck, dbReader)

      logger.info("Initializing HiveManager.")
      val hiveManager = new HiveManager(spark)

      logger.info("Generating schema query.")
      val schemaQuery = QueryGenerator.generateSchemaQuery(tableName, owner)
      logger.info(s"THIS IS SHEMAQUERY: ${schemaQuery}")

      logger.info("Reading schema from Oracle database.")
      val oracleSchema = dbReader.readFromJDBC(schemaQuery)
      logger.info(s"THIS IS ORACLESHEMA: ${oracleSchema}")

      logger.info("Casting schema.")
      val castedSchema = schemaConverter.getColumnInfo(oracleSchema)
      logger.info(s"THIS IS CASTEDSCHEMAA: ${castedSchema}")

      logger.info("Getting future fields.")
      val futureFields = castedSchema.map(info => schemaConverter.convertColumnInfoToStructField(info))
      logger.info(s"THIS IS FUTUREFIELDS: ${futureFields}")

      logger.info("Generating universal query.")
      val universalQuery = QueryGenerator.generateUniversalQuery(owner, tableName)
      logger.info(s"Universal query: $universalQuery")

      logger.info("Getting partition info.")
      val partitionInfo = dbReader.readFromJDBC(universalQuery)

      logger.info("GETTING QUERYCOLUMNS.")
      val queryColumns = oracleSchema.select("COLUMN_NAME").rdd.map(r => r(0)).collect().mkString(", ")
      logger.info(s"THIS IS QUERYCOLUMNS: ${queryColumns}")

      logger.info("Generating data queries.")
      val dataQueries = QueryGenerator.generateDataQuery(queryColumns, owner, tableName, partitionInfo).toIterator

      logger.info("Loading data from Oracle database.")
      implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global
      dbReader.loadData(dataQueries, new StructType(schemaConverter.convert(oracleSchema).toArray)).onComplete {

        case Success(df) =>
          val numRows = df.count()
          logger.info(s"Data loaded successfully. Number of rows: $numRows")

          logger.info("Data loaded successfully.")
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
        case Failure(e) => logger.info(s"Failed to load data from Oracle to Hive. Reason: ${e.getMessage}")
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
