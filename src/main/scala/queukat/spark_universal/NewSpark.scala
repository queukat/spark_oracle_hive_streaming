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
    logger.info(s"${MigrationLogging.stage("INIT")} Creating migration components")
    val spark = SparkSessionFactory.getSparkSession()

    logger.info(s"${MigrationLogging.stage("INIT")} Initializing Oracle reader")
    val dbReader = new DbReader(spark, url, oracleUser, oraclePassword, numPartitions, fetchSize)

    logger.info(s"${MigrationLogging.stage("INIT")} Capturing Oracle snapshot SCN")
    val snapshotScn = dbReader.captureSnapshotScn()

    logger.info(s"${MigrationLogging.stage("INIT")} Initializing schema converter")
    val schemaConverter = new SchemaConverter(spark, owner, tableName, typeCheck, dbReader, Some(snapshotScn))

    logger.info(s"${MigrationLogging.stage("INIT")} Initializing Hive manager")
    val hiveManager = new HiveManager(spark)

    (dbReader, schemaConverter, hiveManager, snapshotScn)
  }

  private def fetchSchema(dbReader: DbReader, tableName: String, owner: String): DataFrame = {
    val schemaQuery = QueryGenerator.generateSchemaQuery(tableName, owner)
    logger.info(s"${MigrationLogging.stage("SCHEMA")} Reading Oracle schema ${MigrationLogging.sqlPreview(schemaQuery)}")
    dbReader.readFromJDBC(schemaQuery)
  }

  private def prepareDataQueries(
    dbReader: DbReader,
    oracleSchema: DataFrame,
    owner: String,
    tableName: String,
    snapshotScn: Long
  ): Seq[String] = {
    val universalQuery = QueryGenerator.generateUniversalQuery(owner, tableName)
    logger.info(s"${MigrationLogging.stage("PLAN")} Reading Oracle extent metadata ${MigrationLogging.sqlPreview(universalQuery)}")
    val partitionInfo = dbReader.readFromJDBC(universalQuery)

    val queryColumns = oracleSchema.select("COLUMN_NAME").collect().map(_.getString(0)).toSeq
    logger.info(
      s"${MigrationLogging.stage("PLAN")} Columns selected for migration " +
        s"${MigrationLogging.kv("count", queryColumns.size)} " +
        s"${MigrationLogging.kv("preview", MigrationLogging.previewList(queryColumns))}"
    )

    val dataQueries = QueryGenerator.generateDataQuery(queryColumns, owner, tableName, partitionInfo, Some(snapshotScn))
    logger.info(s"${MigrationLogging.success("PLAN")} Rowid query plan ready ${MigrationLogging.kv("segments", dataQueries.size)}")
    dataQueries
  }

  private def handleLoadedData(
    df: DataFrame,
    hiveManager: HiveManager,
    hivetable: String,
    numPartitions: Int,
    hiveSchema: StructType
  ): Unit = {
    val tempTableName = s"${hivetable}_temp_${UUID.randomUUID().toString.replace("-", "")}"
    logger.info(
      s"${MigrationLogging.stage("WRITE")} Persisting temporary Hive table " +
        s"${MigrationLogging.kv("tempTable", tempTableName)} " +
        s"${MigrationLogging.kv("columns", hiveSchema.fields.length)}"
    )
    hiveManager.saveAsTemporaryTable(df, tempTableName, numPartitions)

    try {
      logger.info(s"${MigrationLogging.stage("WRITE")} Inserting data into target Hive table ${MigrationLogging.targetTable(hivetable)}")
      hiveManager.insertDataIntoHiveTable(tempTableName, hivetable, hiveSchema)
    } finally {
      logger.info(s"${MigrationLogging.stage("WRITE")} Cleaning up temporary Hive table ${MigrationLogging.kv("tempTable", tempTableName)}")
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
    val startedAt = System.nanoTime()

    try {
      logger.info(
        s"${MigrationLogging.stage("MIGRATE")} Starting Oracle to Hive migration " +
          s"${MigrationLogging.sourceTable(owner, tableName)} -> ${MigrationLogging.targetTable(hivetable)} " +
          s"${MigrationLogging.kv("numPartitions", numPartitions)} " +
          s"${MigrationLogging.kv("fetchSize", fetchSize)} " +
          s"${MigrationLogging.kv("typeCheck", typeCheck)}"
      )

      val (reader, schemaConverter, hiveManager, snapshotScn) =
        createComponents(url, oracleUser, oraclePassword, owner, tableName, numPartitions, fetchSize, typeCheck)
      dbReader = reader

      val oracleSchema = fetchSchema(dbReader, tableName, owner)
      logger.info(s"${MigrationLogging.success("SCHEMA")} Oracle schema DataFrame loaded ${MigrationLogging.kv("columns", oracleSchema.columns.length)}")

      val targetSchema = StructType(schemaConverter.convert(oracleSchema).toArray)
      logger.info(s"${MigrationLogging.success("SCHEMA")} Target schema ready ${MigrationLogging.kv("columns", targetSchema.fields.length)}")

      val dataQueries = prepareDataQueries(dbReader, oracleSchema, owner, tableName, snapshotScn)

      logger.info(s"${MigrationLogging.stage("LOAD")} Loading Oracle data through Spark JDBC")
      val loadedDf = dbReader.loadData(dataQueries.iterator, targetSchema)
      logger.info(
        s"${MigrationLogging.success("LOAD")} Oracle data loaded " +
          s"${MigrationLogging.kv("columns", loadedDf.columns.length)} " +
          s"${MigrationLogging.kv("sparkPartitions", loadedDf.rdd.getNumPartitions)}"
      )

      val (dfToWrite, effectiveSchema) =
        if (typeCheck == "spark") {
          logger.info(s"${MigrationLogging.stage("SCHEMA")} Keeping Spark JDBC inferred schema for write path")
          loadedDf -> loadedDf.schema
        } else {
          logger.info(s"${MigrationLogging.stage("SCHEMA")} Applying explicit target schema before Hive write")
          schemaConverter.castToSchema(loadedDf, targetSchema) -> targetSchema
        }

      logger.info(s"${MigrationLogging.stage("WRITE")} Writing migrated data to Hive")
      handleLoadedData(dfToWrite, hiveManager, hivetable, numPartitions, effectiveSchema)
      logger.info(
        s"${MigrationLogging.success("MIGRATE")} Migration finished successfully " +
          s"${MigrationLogging.kv("duration", MigrationLogging.elapsedSince(startedAt))}"
      )
    } catch {
      case e: Exception =>
        logger.error(
          s"${MigrationLogging.failure("MIGRATE")} Migration failed " +
            s"${MigrationLogging.kv("duration", MigrationLogging.elapsedSince(startedAt))} " +
            s"${MigrationLogging.kv("reason", e.getMessage)}",
          e
        )
        throw e
    } finally {
      if (dbReader != null) {
        dbReader.close()
      }
    }
  }
}
